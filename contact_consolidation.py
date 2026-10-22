"""Prepare an owner-approved contact merge offline; execute SQL via Supabase.

No database/application imports or credentials. Child rows are never deduplicated
or deleted. The SQL aborts if contacts, dependency manifests or schema changed.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
from datetime import date, datetime, timezone
import hashlib
import json
import re
from uuid import UUID


REFERENCE_TABLES = ('account_contact', 'advisor', 'contact_document', 'contact_screening',
                    'document_review_email', 'document_review_responsible',
                    'investment_proposal', 'risk_profile', 'user',
                    'contact_identity', 'contact_identity_document')
IDENTITY_TABLES = ('contact_identity', 'contact_identity_document')
IDENTITY_LOCK = int.from_bytes(hashlib.sha256(b'application-contact-identity-v1').digest()[:8], 'big', signed=True)
FIELDS = ('name', 'type', 'email', 'phone', 'met_place')


def present(value):
    return value is not None and str(value).strip().lower() not in {'', '-', 'none', 'null'}


def completeness(contact):
    meaningful = sum(present(contact.get(f)) for f in FIELDS)
    if contact.get('type') not in {'person', 'company'} and present(contact.get('type')):
        meaningful -= 1
    return meaningful


def manifest_digest(rows, mapping=None, table=None):
    mapping = mapping or {}
    transformed = [(mapping.get(r['contact_id'], r['contact_id']) if table == 'contact_identity' else r['id'],
                    mapping.get(r['contact_id'], r['contact_id']), r['fingerprint']) for r in rows]
    content = '\n'.join(f'{key}|{contact_id}|{fingerprint}' for key, contact_id, fingerprint in sorted(transformed))
    return hashlib.md5(content.encode()).hexdigest()


def validate_identity_snapshot(backup, contacts, refs):
    evidence = backup.get('identity')
    if not isinstance(evidence, dict) or set(evidence) != set(IDENTITY_TABLES):
        raise ValueError('Fresh full identity snapshots are required, including explicit empty arrays')
    for table in IDENTITY_TABLES:
        rows = evidence[table]
        if not isinstance(rows, list):
            raise ValueError('Identity snapshot must be a row list')
        key = 'contact_id' if table == 'contact_identity' else 'id'
        fields = {'contact_id', 'source_account_id', 'source', 'verification_status', 'created', 'updated'}
        fields |= {'date_of_birth'} if table == 'contact_identity' else {'id', 'document_type', 'issuing_country', 'identifier'}
        if any(set(row) != fields for row in rows):
            raise ValueError('Identity snapshot columns changed; refresh the script/catalog')
        if len({row[key] for row in rows}) != len(rows):
            raise ValueError('Duplicate identity snapshot keys')
        if {(row[key], row['contact_id']) for row in rows} != {(row['id'], row['contact_id']) for row in refs[table]}:
            raise ValueError('Identity rows and dependency snapshot disagree')
        for row in rows:
            if row['contact_id'] not in contacts or contacts[row['contact_id']].get('type') == 'company':
                raise ValueError('Identity evidence belongs outside the approved people scope')
            if row['source'] != 'application' or row['verification_status'] != 'applicant_declared':
                raise ValueError('Unsupported identity provenance; review required')
            if row['source_account_id'] is not None:
                UUID(row['source_account_id'])
            if table == 'contact_identity':
                date.fromisoformat(row['date_of_birth'])
            elif row['document_type'] not in {'passport', 'nationalCard', 'driversLicense'} or not re.fullmatch(r'[A-Z]{3}', row['issuing_country']) or not re.fullmatch(r'[A-Z0-9]{1,128}', row['identifier']):
                raise ValueError('Invalid scoped identity document')
    return deepcopy(evidence)


def build_plan(backup):
    contacts = {c['id']: c for c in backup['contacts']['contacts']}
    if len(contacts) != len(backup['contacts']['contacts']):
        raise ValueError('Duplicate contact IDs in snapshot')
    decisions = backup['decisions']
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    for pair in decisions['confirmed_duplicate_people']:
        a, b = pair['left_contact_id'], pair['right_contact_id']
        if a not in contacts or b not in contacts:
            raise ValueError('Approved contact is missing; refresh scope before planning')
        parent[find(a)] = find(b)
    groups = defaultdict(list)
    for cid in list(parent):
        groups[find(cid)].append(cid)
    for pair in decisions['different_people']:
        if find(pair['left_contact_id']) == find(pair['right_contact_id']):
            raise ValueError('Contradictory owner decisions')
    if set(contacts) != {cid for group in groups.values() for cid in group}:
        raise ValueError('Snapshot contains contacts outside approved merge groups')
    refs = backup['references']['references']
    if set(refs) != set(REFERENCE_TABLES):
        raise ValueError('Unexpected dependency scope')
    totals = backup['references']['reference_totals']
    if set(totals) != set(REFERENCE_TABLES) or any(type(n) is not int or n < len(refs[table]) for table, n in totals.items()):
        raise ValueError('Missing or invalid dependency totals')
    for contact_id in contacts:
        if str(UUID(contact_id)) != contact_id:
            raise ValueError('Canonical contact UUIDs are required')
    for name in ('contact', 'document'):
        if type(backup['contacts']['totals'][name]) is not int or backup['contacts']['totals'][name] < 0:
            raise ValueError('Invalid population totals')
    for table, rows in refs.items():
        for row in rows:
            UUID(row['id'])
            if row['contact_id'] not in contacts or not re.fullmatch(r'[0-9a-f]{32}', row['fingerprint']):
                raise ValueError('Dependency row is outside approved scope or has invalid fingerprint')
            if table == 'contact_identity' and row['id'] != row['contact_id']:
                raise ValueError('Birthday dependency key must equal contact_id')
    identity = validate_identity_snapshot(backup, contacts, refs)
    ref_counts = Counter(r['contact_id'] for rows in refs.values() for r in rows)
    mapping, survivors, group_records = {}, [], []
    stamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    for ids in sorted(groups.values(), key=lambda g: sorted(g)):
        ranked = sorted(ids, key=lambda cid: (-completeness(contacts[cid]), -ref_counts[cid],
                        -len(str(contacts[cid].get('name') or '').split()),
                        -len(str(contacts[cid].get('name') or '')), cid))
        keep = ranked[0]
        final = deepcopy(contacts[keep])
        for field in FIELDS:
            if not present(final.get(field)):
                value = next((contacts[cid][field] for cid in ranked if present(contacts[cid].get(field))), None)
                if value is not None:
                    final[field] = value
        types = {contacts[cid].get('type') for cid in ids} & {'company', 'person'}
        if len(types) > 1:
            raise ValueError('Approved group mixes person and company; cannot choose canonical type safely')
        if len(types) == 1:
            final['type'] = next(iter(types))
        # Preserve a fuller approved name when one is simply an expansion of
        # the retained record's name. Never synthesize a name or other value.
        tokens = lambda c: set(re.findall(r'\w+', str(c.get('name') or '').casefold()))
        expansions = [contacts[cid] for cid in ids if tokens(final) <= tokens(contacts[cid])]
        if expansions:
            final['name'] = max(expansions, key=lambda c: (len(tokens(c)), len(c['name'])))['name']
        final['updated'] = stamp
        survivors.append(final)
        for cid in ids:
            mapping[cid] = keep
        group_records.append({'survivor_id': keep, 'merged_ids': sorted(cid for cid in ids if cid != keep),
                              'completeness_scores': {cid: completeness(contacts[cid]) for cid in ids},
                              'reference_counts': {cid: ref_counts[cid] for cid in ids},
                              'field_alternatives': {f: list(dict.fromkeys(str(contacts[cid][f]) for cid in ids if present(contacts[cid].get(f)))) for f in FIELDS},
                              'final_contact': final})
    for pair in decisions['different_people']:
        a, b = pair['left_contact_id'], pair['right_contact_id']
        if mapping.get(a, a) == mapping.get(b, b):
            raise ValueError('Plan would combine owner-rejected pair')
    companies = {c['id'] for c in survivors if c.get('type') == 'company'}
    for group in group_records:
        members = {group['survivor_id'], *group['merged_ids']}
        birthdays = [row for row in identity['contact_identity'] if row['contact_id'] in members]
        documents = [row for row in identity['contact_identity_document'] if row['contact_id'] in members]
        if group['survivor_id'] in companies and (birthdays or documents):
            raise ValueError('Company group contains person identity evidence')
        if len({row['date_of_birth'] for row in birthdays}) > 1:
            raise ValueError('Conflicting birthdays require identity review')
        if len(birthdays) > 1:
            raise ValueError('Multiple birthday records require reviewed provenance resolution; no identity row is discarded')
        scoped = defaultdict(dict)
        for row in documents:
            values = scoped[(row['document_type'], row['issuing_country'])].setdefault(row['contact_id'], set())
            values.add(row['identifier'])
        for owners in scoped.values():
            if len(owners) > 1:
                raise ValueError('Scoped government IDs on multiple contacts require identity review')
        group['identity_preservation'] = {'birthday_rows': len(birthdays), 'document_rows': len(documents),
                                          'policy': 'Repoint contact_id only; retain every identity/provenance row. Multiple birthday rows or competing scoped IDs block planning.'}
    if any(mapping[l['contact_id']] in companies and l.get('entity_id') is not None
           for l in backup['extra']['account_links']):
        raise ValueError('Company link still carries person entity ID; handle it explicitly before merge')
    manifests = {}
    for table, rows in refs.items():
        if len({r['id'] for r in rows}) != len(rows):
            raise ValueError('Duplicate dependency row IDs')
        manifests[table] = {'rows': len(rows), 'before': manifest_digest(rows, table=table),
                            'after': manifest_digest(rows, mapping, table),
                            'repointed_rows': sum(mapping[r['contact_id']] != r['contact_id'] for r in rows)}
    plan = {'schema_version': 2, 'mapping': mapping, 'survivors': sorted(survivors, key=lambda c: c['id']),
            'identity_before': {table: sorted(rows, key=lambda row: row.get('id', row['contact_id'])) for table, rows in identity.items()},
            'groups': group_records, 'manifests': manifests,
            'delete_ids': sorted(cid for cid, target in mapping.items() if cid != target),
            'before_contacts': sorted(contacts.values(), key=lambda c: c['id']),
            'before_total_contacts': backup['contacts']['totals']['contact'],
            'before_total_documents': backup['contacts']['totals']['document'],
            'reference_totals': backup['references']['reference_totals'],
            'selection_policy': 'Most populated business fields, then most existing references, then fuller name, then stable ID. Fill missing fields from approved members, retain chosen populated email/phone, expand names only from an approved superset, normalize known person/company type. Preserve alternatives in private backup.'}
    plan['sha256'] = hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return plan


def identity_snapshot_sql(contact_ids):
    """Read-only adjunct; run inside the SAME snapshot as the other backup reads.

    Returns identity rows plus reference manifests/totals to incorporate into
    the reviewed backup. This does not execute queries or generate approvals.
    """
    ids = sorted({str(UUID(value)) for value in contact_ids})
    if not ids:
        raise ValueError('Explicit approved contact scope is required')
    scope = 'ARRAY[' + ','.join("'" + value + "'::uuid" for value in ids) + ']'
    evidence, references, totals = [], [], []
    for table in IDENTITY_TABLES:
        key = 'contact_id' if table == 'contact_identity' else 'id'
        evidence.append(f"'{table}', (SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY t.{key}),'[]'::jsonb) FROM public.{table} t WHERE contact_id=ANY({scope}))")
        references.append(f"'{table}', (SELECT coalesce(jsonb_agg(jsonb_build_object('id',t.{key},'contact_id',t.contact_id,'fingerprint',md5((to_jsonb(t)-'contact_id')::text)) ORDER BY t.{key}),'[]'::jsonb) FROM public.{table} t WHERE contact_id=ANY({scope}))")
        totals.append(f"'{table}', (SELECT count(*) FROM public.{table})")
    return 'SELECT jsonb_build_object(\n' + ',\n'.join([
        "'identity', jsonb_build_object(" + ','.join(evidence) + ')',
        "'identity_references', jsonb_build_object(" + ','.join(references) + ')',
        "'identity_totals', jsonb_build_object(" + ','.join(totals) + ')']) + ') AS identity_backup;'


def sql_json(value):
    return "'" + json.dumps(value, ensure_ascii=False).replace("'", "''") + "'::jsonb"


def manifest_sql(table):
    if table not in REFERENCE_TABLES:
        raise ValueError('Unapproved table')
    key = 'contact_id' if table == 'contact_identity' else 'id'
    return f'''SELECT md5(coalesce(string_agg(t.{key}::text || '|' || t.contact_id::text || '|' || md5((to_jsonb(t)-'contact_id')::text), E'\\n' ORDER BY t.{key}),''))
               FROM public."{table}" t WHERE t.contact_id = ANY(scope_ids)'''


def render_sql(plan, commit=False):
    # Validate fingerprint before generating any executable statement.
    body = {k: v for k, v in plan.items() if k != 'sha256'}
    expected = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if expected != plan['sha256']:
        raise ValueError('Plan hash mismatch')
    if plan.get('schema_version') != 2 or set(plan['manifests']) != set(REFERENCE_TABLES):
        raise ValueError('Fresh schema-version-2 plan required; legacy plans cannot be rendered')
    checks, updates, final_checks = [], [], []
    for table in IDENTITY_TABLES:
        key = 'contact_id' if table == 'contact_identity' else 'id'
        checks.append(f'''IF (SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY t.{key}), '[]'::jsonb) FROM public."{table}" t WHERE contact_id=ANY(scope_ids)) IS DISTINCT FROM {sql_json(plan['identity_before'][table])} THEN RAISE EXCEPTION 'Identity evidence changed in {table}'; END IF;''')
    for table, manifest in plan['manifests'].items():
        statement = manifest_sql(table)
        key = 'contact_id' if table == 'contact_identity' else 'id'
        checks.append(f'''PERFORM 1 FROM public."{table}" WHERE contact_id=ANY(scope_ids) ORDER BY {key} FOR UPDATE;
 IF ({statement}) <> '{manifest['before']}' THEN RAISE EXCEPTION 'Dependency drift in {table}'; END IF;''')
        updates.append(f'''UPDATE public."{table}" t SET contact_id=(m.value #>> '{{}}')::uuid
 FROM jsonb_each(mapping) m WHERE t.contact_id=m.key::uuid AND m.key <> m.value #>> '{{}}';
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed <> {manifest['repointed_rows']} THEN RAISE EXCEPTION 'Unexpected update count in {table}'; END IF;''')
        final_checks.append(f'''IF ({statement}) <> '{manifest['after']}' THEN RAISE EXCEPTION 'Dependency preservation failed in {table}'; END IF;
 IF (SELECT count(*) FROM public."{table}") <> {plan['reference_totals'][table]} THEN RAISE EXCEPTION 'Row loss in {table}'; END IF;
 IF EXISTS (SELECT 1 FROM public."{table}" WHERE contact_id=ANY(removed_ids)) THEN RAISE EXCEPTION 'Old reference remains in {table}'; END IF;''')
    delimiter = '$contact_consolidation$'
    while delimiter in json.dumps(plan):
        delimiter = delimiter[:-1] + '_$'
    return f'''-- Owner-reviewed confirmed-contact consolidation. Execution requires separate approval. Plan SHA-256: {plan['sha256']}
BEGIN ISOLATION LEVEL SERIALIZABLE;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='90s';
DO {delimiter}
DECLARE
 mapping jsonb := {sql_json(plan['mapping'])};
 originals jsonb := {sql_json(plan['before_contacts'])};
 finals jsonb := {sql_json(plan['survivors'])};
 scope_ids uuid[];
 removed_ids uuid[];
 changed integer;
BEGIN
 IF NOT pg_try_advisory_xact_lock(hashtextextended('agm-confirmed-contact-consolidation',0)) THEN RAISE EXCEPTION 'Another merge is active'; END IF;
 IF NOT pg_try_advisory_xact_lock({IDENTITY_LOCK}::bigint) THEN RAISE EXCEPTION 'Application identity reconciliation is active'; END IF;
 LOCK TABLE public.contact, {', '.join('public."' + table + '"' for table in sorted(REFERENCE_TABLES))} IN SHARE ROW EXCLUSIVE MODE;
 SELECT array_agg(key::uuid ORDER BY key) INTO scope_ids FROM jsonb_each(mapping);
 SELECT array_agg(key::uuid ORDER BY key) INTO removed_ids FROM jsonb_each(mapping) WHERE key <> value #>> '{{}}';
 IF (SELECT count(*) FROM pg_constraint WHERE contype='f' AND confrelid='public.contact'::regclass) <> {len(REFERENCE_TABLES)} THEN RAISE EXCEPTION 'Contact reference schema changed'; END IF;
 IF EXISTS (SELECT 1 FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE k.contype='f' AND k.confrelid='public.contact'::regclass AND (n.nspname<>'public' OR c.relname NOT IN ({','.join(repr(t) for t in REFERENCE_TABLES)}) OR cardinality(k.conkey)<>1 OR NOT EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid=k.conrelid AND a.attnum=k.conkey[1] AND a.attname='contact_id'))) THEN RAISE EXCEPTION 'Unexpected foreign key'; END IF;
 IF EXISTS (SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND (tgrelid='public.contact'::regclass OR tgrelid IN (SELECT conrelid FROM pg_constraint WHERE contype='f' AND confrelid='public.contact'::regclass))) THEN RAISE EXCEPTION 'Unexpected mutation trigger'; END IF;
 PERFORM 1 FROM public.contact WHERE id=ANY(scope_ids) ORDER BY id FOR UPDATE;
 IF (SELECT jsonb_agg(to_jsonb(c) ORDER BY c.id) FROM public.contact c WHERE id=ANY(scope_ids)) IS DISTINCT FROM originals THEN RAISE EXCEPTION 'Contact records changed since backup'; END IF;
 IF (SELECT count(*) FROM public.contact) <> {plan['before_total_contacts']} THEN RAISE EXCEPTION 'Contact population changed; refresh plan'; END IF;
 {' '.join(checks)}
 UPDATE public.contact c SET name=f->>'name',type=f->>'type',email=f->>'email',phone=f->>'phone',met_place=f->>'met_place',updated=f->>'updated'
 FROM jsonb_array_elements(finals) f WHERE c.id=(f->>'id')::uuid;
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed <> {len(plan['survivors'])} THEN RAISE EXCEPTION 'Survivor update count mismatch'; END IF;
 {' '.join(updates)}
 {' '.join(final_checks)}
 DELETE FROM public.contact WHERE id=ANY(removed_ids);
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed <> {len(plan['delete_ids'])} THEN RAISE EXCEPTION 'Redundant contact deletion count mismatch'; END IF;
 {' '.join(final_checks)}
 IF (SELECT jsonb_agg(to_jsonb(c) ORDER BY c.id) FROM public.contact c WHERE id=ANY(scope_ids)) IS DISTINCT FROM finals THEN RAISE EXCEPTION 'Canonical contacts differ from plan'; END IF;
 IF (SELECT count(*) FROM public.contact) <> {plan['before_total_contacts']-len(plan['delete_ids'])} THEN RAISE EXCEPTION 'Contact count mismatch'; END IF;
 IF (SELECT count(*) FROM public.document) <> {plan['before_total_documents']} THEN RAISE EXCEPTION 'Raw document count changed'; END IF;
END {delimiter};
SELECT json_build_object('plan_sha256','{plan['sha256']}','surviving_groups',{len(plan['survivors'])},'redundant_contacts_removed',{len(plan['delete_ids'])},'all_dependency_manifests_verified',true,'mode','{'commit' if commit else 'rollback_rehearsal'}') AS verified_result;
{'COMMIT' if commit else 'ROLLBACK'};
'''
