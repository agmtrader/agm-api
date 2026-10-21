"""Merge planning controls; the actual database SQL is rehearsed with ROLLBACK."""
import copy
import hashlib
import importlib.util
from pathlib import Path

import pytest

path = Path(__file__).resolve().parents[1] / 'contact_consolidation.py'
spec = importlib.util.spec_from_file_location('contact_consolidation', path)
merge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(merge)


def backup():
    a, b, c = ('00000000-0000-4000-8000-00000000000' + str(i) for i in range(1, 4))
    contacts = [{'id': a, 'name': 'Ana Lopez', 'type': None, 'email': None, 'phone': None,
                 'met_place': 'Referral', 'created': '', 'updated': ''},
                {'id': b, 'name': 'Ana Maria Lopez', 'type': 'person', 'email': 'ana@example.test',
                 'phone': '5551234567', 'met_place': None, 'created': '20200101', 'updated': ''}]
    refs = {t: [] for t in merge.REFERENCE_TABLES}
    refs['account_contact'] = [{'id': '00000000-0000-4000-8000-000000000011', 'contact_id': a, 'fingerprint': 'a' * 32},
                               {'id': '00000000-0000-4000-8000-000000000012', 'contact_id': b, 'fingerprint': 'b' * 32}]
    return {'contacts': {'contacts': contacts, 'totals': {'contact': 3, 'document': 5}},
            'references': {'references': refs, 'reference_totals': {t: len(v) for t, v in refs.items()}},
            'extra': {'account_links': [{'id': r['id'], 'contact_id': r['contact_id'], 'entity_id': 123} for r in refs['account_contact']]},
            'decisions': {'confirmed_duplicate_people': [{'left_contact_id': a, 'right_contact_id': b}],
                          'different_people': [{'left_contact_id': a, 'right_contact_id': c}]}}


def test_retains_complete_contact_and_fills_missing_field_without_mutating_backup():
    b = backup(); original = copy.deepcopy(b)
    plan = merge.build_plan(b)
    assert plan['delete_ids'] == [b['contacts']['contacts'][0]['id']]
    survivor = plan['survivors'][0]
    assert survivor['id'] == b['contacts']['contacts'][1]['id']
    assert survivor['email'] == 'ana@example.test'
    assert survivor['met_place'] == 'Referral'
    assert b == original


def test_every_link_row_is_preserved_even_if_merging_makes_identical_relationships():
    b = backup(); plan = merge.build_plan(b)
    manifest = plan['manifests']['account_contact']
    assert manifest['rows'] == 2
    assert manifest['repointed_rows'] == 1
    rows = b['references']['references']['account_contact']
    survivor = plan['survivors'][0]['id']
    expected = hashlib.md5(('\n'.join(f"{r['id']}|{survivor}|{r['fingerprint']}" for r in rows)).encode()).hexdigest()
    assert manifest['after'] == expected
    sql = merge.render_sql(plan)
    assert 'DELETE FROM public.contact WHERE id=ANY(removed_ids)' in sql
    assert 'DELETE FROM public."account_contact"' not in sql
    assert 'Dependency preservation failed in contact_screening' in sql
    assert sql.rstrip().endswith('ROLLBACK;')


def test_conflicting_owner_decisions_and_missing_contacts_block_plan():
    b = backup()
    b['decisions']['different_people'] = b['decisions']['confirmed_duplicate_people']
    with pytest.raises(ValueError, match='Contradictory'):
        merge.build_plan(b)
    b = backup(); b['contacts']['contacts'].pop()
    with pytest.raises(ValueError, match='missing'):
        merge.build_plan(b)


def test_transitive_confirmations_choose_one_survivor_and_preserve_negative_pairs():
    b = backup(); c = copy.deepcopy(b['contacts']['contacts'][0]); c.update(id='00000000-0000-4000-8000-000000000004')
    b['contacts']['contacts'].append(c)
    b['decisions']['confirmed_duplicate_people'].append({'left_contact_id': b['contacts']['contacts'][1]['id'], 'right_contact_id': c['id']})
    plan = merge.build_plan(b)
    assert len(plan['groups']) == 1 and len(plan['delete_ids']) == 2
    assert len(set(plan['mapping'].values())) == 1


def test_company_identity_and_ambiguous_company_person_groups_block_plan():
    b = backup(); b['contacts']['contacts'][0]['type'] = 'company'
    with pytest.raises(ValueError, match='mixes person and company'):
        merge.build_plan(b)
    b['contacts']['contacts'][1]['type'] = 'company'
    with pytest.raises(ValueError, match='Company link'):
        merge.build_plan(b)


def test_changed_plan_is_rejected_and_commit_requires_explicit_generation():
    plan = merge.build_plan(backup())
    assert merge.render_sql(plan, commit=True).rstrip().endswith('COMMIT;')
    plan['survivors'][0]['email'] = 'changed@example.test'
    with pytest.raises(ValueError, match='hash mismatch'):
        merge.render_sql(plan, commit=True)
