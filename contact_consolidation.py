"""Prepare an owner-approved contact merge offline; execute SQL via Supabase.

No database/application imports or credentials. Child rows are never deduplicated
or deleted. The SQL aborts if contacts, dependency manifests or schema changed.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import re


REFERENCE_TABLES = ('account_contact', 'advisor', 'contact_document', 'contact_screening',
                    'document_review_email', 'document_review_responsible',
                    'investment_proposal', 'risk_profile', 'user')
FIELDS = ('name', 'type', 'email', 'phone', 'met_place')


def present(value):
    return value is not None and str(value).strip().lower() not in {'', '-', 'none', 'null'}


def completeness(contact):
    meaningful = sum(present(contact.get(f)) for f in FIELDS)
    if contact.get('type') not in {'person', 'company'} and present(contact.get('type')):
        meaningful -= 1
    return meaningful


def manifest_digest(rows, mapping=None):
    mapping = mapping or {}
    content = '\n'.join(f"{r['id']}|{mapping.get(r['contact_id'], r['contact_id'])}|{r['fingerprint']}"
                        for r in sorted(rows, key=lambda r: r['id']))
    return hashlib.md5(content.encode()).hexdigest()


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
    if any(mapping[l['contact_id']] in companies and l.get('entity_id') is not None
           for l in backup['extra']['account_links']):
        raise ValueError('Company link still carries person entity ID; handle it explicitly before merge')
    manifests = {}
    for table, rows in refs.items():
        if len({r['id'] for r in rows}) != len(rows):
            raise ValueError('Duplicate dependency row IDs')
        manifests[table] = {'rows': len(rows), 'before': manifest_digest(rows),
                            'after': manifest_digest(rows, mapping),
                            'repointed_rows': sum(mapping[r['contact_id']] != r['contact_id'] for r in rows)}
    plan = {'schema_version': 1, 'mapping': mapping, 'survivors': sorted(survivors, key=lambda c: c['id']),
            'groups': group_records, 'manifests': manifests,
            'delete_ids': sorted(cid for cid, target in mapping.items() if cid != target),
            'before_contacts': sorted(contacts.values(), key=lambda c: c['id']),
            'before_total_contacts': backup['contacts']['totals']['contact'],
            'before_total_documents': backup['contacts']['totals']['document'],
            'reference_totals': backup['references']['reference_totals'],
            'selection_policy': 'Most populated business fields, then most existing references, then fuller name, then stable ID. Fill missing fields from approved members, retain chosen populated email/phone, expand names only from an approved superset, normalize known person/company type. Preserve alternatives in private backup.'}
    plan['sha256'] = hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return plan


def sql_json(value):
    return "'" + json.dumps(value, ensure_ascii=False).replace("'", "''") + "'::jsonb"


def manifest_sql(table):
    if table not in REFERENCE_TABLES:
        raise ValueError('Unapproved table')
    return f'''SELECT md5(coalesce(string_agg(t.id::text || '|' || t.contact_id::text || '|' || md5((to_jsonb(t)-'contact_id')::text), E'\\n' ORDER BY t.id),''))
               FROM public."{table}" t WHERE t.contact_id = ANY(scope_ids)'''


def render_sql(plan, commit=False):
    # Validate fingerprint before generating any executable statement.
    body = {k: v for k, v in plan.items() if k != 'sha256'}
    expected = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if expected != plan['sha256']:
        raise ValueError('Plan hash mismatch')
    checks, updates, final_checks = [], [], []
    for table, manifest in plan['manifests'].items():
        statement = manifest_sql(table)
        checks.append(f'''PERFORM 1 FROM public."{table}" WHERE contact_id=ANY(scope_ids) ORDER BY id FOR UPDATE;
 IF ({statement}) <> '{manifest['before']}' THEN RAISE EXCEPTION 'Dependency drift in {table}'; END IF;''')
        updates.append(f'''UPDATE public."{table}" t SET contact_id=(m.value #>> '{{}}')::uuid
 FROM jsonb_each(mapping) m WHERE t.contact_id=m.key::uuid AND m.key <> m.value #>> '{{}}';
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed <> {manifest['repointed_rows']} THEN RAISE EXCEPTION 'Unexpected update count in {table}'; END IF;''')
        final_checks.append(f'''IF ({statement}) <> '{manifest['after']}' THEN RAISE EXCEPTION 'Dependency preservation failed in {table}'; END IF;
 IF (SELECT count(*) FROM public."{table}") <> {plan['reference_totals'][table]} THEN RAISE EXCEPTION 'Row loss in {table}'; END IF;
 IF EXISTS (SELECT 1 FROM public."{table}" WHERE contact_id=ANY(removed_ids)) THEN RAISE EXCEPTION 'Old reference remains in {table}'; END IF;''')
    return f'''-- Owner-authorized confirmed-contact consolidation. Plan SHA-256: {plan['sha256']}
BEGIN ISOLATION LEVEL SERIALIZABLE;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='90s';
DO $contact_consolidation$
DECLARE
 mapping jsonb := {sql_json(plan['mapping'])};
 originals jsonb := {sql_json(plan['before_contacts'])};
 finals jsonb := {sql_json(plan['survivors'])};
 scope_ids uuid[];
 removed_ids uuid[];
 changed integer;
BEGIN
 IF NOT pg_try_advisory_xact_lock(hashtextextended('agm-confirmed-contact-consolidation',0)) THEN RAISE EXCEPTION 'Another merge is active'; END IF;
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
END $contact_consolidation$;
SELECT json_build_object('plan_sha256','{plan['sha256']}','surviving_groups',{len(plan['survivors'])},'redundant_contacts_removed',{len(plan['delete_ids'])},'all_dependency_manifests_verified',true,'mode','{'commit' if commit else 'rollback_rehearsal'}') AS verified_result;
{'COMMIT' if commit else 'ROLLBACK'};
'''
