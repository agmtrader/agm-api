"""Read-only aggregate review of stored contact-document metadata."""

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
    with database.engine.connect() as connection:
        rows = connection.execute(QUERY).mappings().all()
    return build_review(rows)
