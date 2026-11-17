"""Read-only aggregate review of stored documents, contacts and OCR."""

from datetime import datetime, timezone

from sqlalchemy import text


QUERY = text("""
    SELECT coalesce(nullif(trim(category), ''), '[blank]') AS category,
           count(*) AS total,
           count(*) FILTER (WHERE nullif(trim(type), '') IS NULL) AS missing_type,
           count(*) FILTER (WHERE nullif(trim(issued_date), '') IS NULL) AS missing_issue_date,
           count(*) FILTER (WHERE nullif(trim(expiry_date), '') IS NULL) AS missing_expiry_date
    FROM contact_document
    GROUP BY 1 ORDER BY 1
""")

INVENTORY = text("""
    WITH checksums AS (
        SELECT sha1_checksum, count(*) AS members
        FROM document WHERE nullif(trim(sha1_checksum), '') IS NOT NULL
        GROUP BY 1 HAVING count(*) > 1
    ), linked AS (
        SELECT DISTINCT document_id FROM contact_document
    ), ocr AS (
        SELECT document_id,
               bool_or(status = 'completed' AND nullif(trim(output_text), '') IS NOT NULL) AS complete,
               bool_or(status IN ('failed', 'error')) AS failed
        FROM document_processing WHERE process_type = 'text_extraction'
        GROUP BY 1
    ), signals AS (
        SELECT id AS contact_id, 'email' AS signal, lower(trim(email)) AS value
        FROM contact WHERE nullif(trim(email), '') IS NOT NULL
        UNION ALL
        SELECT id, 'phone', regexp_replace(phone, '[^0-9]', '', 'g')
        FROM contact WHERE length(regexp_replace(phone, '[^0-9]', '', 'g')) >= 7
        UNION ALL
        SELECT c.id, 'name and birthday', lower(regexp_replace(trim(c.name), '\\s+', ' ', 'g'))
               || ':' || i.date_of_birth::text
        FROM contact c JOIN contact_identity i ON i.contact_id = c.id
        WHERE nullif(trim(c.name), '') IS NOT NULL AND i.date_of_birth < current_date
    ), shared_signals AS (
        SELECT signal, value FROM signals GROUP BY 1, 2 HAVING count(DISTINCT contact_id) > 1
    )
    SELECT
        (SELECT count(*) FROM document) AS documents,
        (SELECT count(*) FROM document d WHERE NOT EXISTS
         (SELECT 1 FROM linked l WHERE l.document_id = d.id)) AS unlinked_documents,
        (SELECT count(*) FROM document WHERE nullif(trim(sha1_checksum), '') IS NULL) AS missing_checksums,
        (SELECT count(*) FROM checksums) AS duplicate_checksum_groups,
        (SELECT coalesce(sum(members), 0) FROM checksums) AS documents_in_duplicate_groups,
        (SELECT count(*) FROM contact) AS contacts,
        (SELECT count(*) FROM shared_signals WHERE signal = 'email') AS shared_email_groups,
        (SELECT count(*) FROM shared_signals WHERE signal = 'phone') AS shared_phone_groups,
        (SELECT count(*) FROM shared_signals WHERE signal = 'name and birthday') AS shared_name_birthday_groups,
        (SELECT count(DISTINCT s.contact_id) FROM signals s JOIN shared_signals USING (signal, value))
            AS contacts_with_shared_signals,
        (SELECT count(*) FROM linked JOIN document d ON d.id = linked.document_id) AS linked_documents,
        (SELECT count(*) FROM linked JOIN document d ON d.id = linked.document_id
         JOIN ocr ON ocr.document_id = d.id WHERE ocr.complete) AS ocr_complete,
        (SELECT count(*) FROM linked JOIN document d ON d.id = linked.document_id
         JOIN ocr ON ocr.document_id = d.id WHERE NOT ocr.complete AND ocr.failed) AS ocr_failed,
        (SELECT count(*) FROM linked JOIN document d ON d.id = linked.document_id
         JOIN ocr ON ocr.document_id = d.id WHERE NOT ocr.complete AND NOT ocr.failed) AS ocr_incomplete,
        (SELECT count(*) FROM linked JOIN document d ON d.id = linked.document_id
         LEFT JOIN ocr ON ocr.document_id = d.id WHERE ocr.document_id IS NULL) AS ocr_no_record
""")


def build_inventory(row):
    return {
        'unlinked_documents': {
            'documents': int(row['documents']),
            'unlinked': int(row['unlinked_documents']),
        },
        'duplicate_checksums': {
            'groups': int(row['duplicate_checksum_groups']),
            'documents_in_groups': int(row['documents_in_duplicate_groups']),
            'missing_checksums': int(row['missing_checksums']),
        },
        'duplicate_contact_candidates': {
            'contacts': int(row['contacts']),
            'contacts_with_shared_signals': int(row['contacts_with_shared_signals']),
            'shared_email_groups': int(row['shared_email_groups']),
            'shared_phone_groups': int(row['shared_phone_groups']),
            'shared_name_birthday_groups': int(row['shared_name_birthday_groups']),
            'coverage': 'Stored AGM signals only. Groups can overlap and require human review. '
                        'IBKR holder evidence and manual identity reconciliation are separate.',
        },
        'ocr_coverage': {
            'linked_documents': int(row['linked_documents']),
            'complete': int(row['ocr_complete']),
            'failed_without_completion': int(row['ocr_failed']),
            'other_incomplete': int(row['ocr_incomplete']),
            'no_extraction_record': int(row['ocr_no_record']),
        },
    }


def build_review(rows):
    """Report observable blanks without claiming they are compliance violations."""
    results = []
    for row in rows:
        category = row['category']
        key = category.strip().casefold()
        checks = {}
        if key in {'proof of identity', 'proof of address', 'source of wealth'}:
            checks['missing_type'] = int(row['missing_type'])
        if key == 'proof of identity':
            checks['missing_expiry_date'] = int(row['missing_expiry_date'])
        if key == 'proof of address':
            checks['missing_issue_date'] = int(row['missing_issue_date'])
        results.append({'category': category, 'total': int(row['total']), 'checks': checks})
    return {
        'status': 'success',
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'total_links': sum(row['total'] for row in results),
        'categories': results,
        'notice': 'Blank metadata counts are review indicators, not confirmed violations. Other categories require rules.',
    }


def review_document_metadata(database):
    with database.engine.connect().execution_options(isolation_level='REPEATABLE READ') as connection:
        connection.exec_driver_sql('SET TRANSACTION READ ONLY')
        rows = connection.execute(QUERY).mappings().all()
        inventory = connection.execute(INVENTORY).mappings().one()
    return {**build_review(rows), **build_inventory(inventory)}
