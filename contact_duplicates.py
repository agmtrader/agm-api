"""Offline, read-only duplicate-contact review using explicit DB and IBKR snapshots.

No application imports, credentials, network access, apply mode or merge decisions.
Run with --help. Output contains personal information: keep it private.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from difflib import SequenceMatcher
import hashlib
from itertools import combinations
import json
import os
from pathlib import Path
import re
import unicodedata


def norm(value):
    text = unicodedata.normalize('NFKD', str(value or '')).casefold()
    return ' '.join(re.sub(r'[^a-z0-9]+', ' ', ''.join(c for c in text if not unicodedata.combining(c))).split())


def identifier(value):
    # Keep leading zeroes; ignore formatting separators only.
    return re.sub(r'[^A-Z0-9]', '', str(value or '').upper())


def meaningful_id(value):
    v = identifier(value)
    return v if v and not re.fullmatch(r'0+|X+', v) and v not in {'NONE', 'NULL', 'NA', 'UNKNOWN', 'TEST'} else ''


def recent_source(source):
    try:
        stamp = datetime.fromisoformat(source['last_success_at'].replace('Z', '+00:00'))
        if stamp.tzinfo is None:
            return False
        age = (datetime.now(timezone.utc) - stamp).total_seconds()
        return source['refresh_status'] == 'fresh' and 0 <= age <= 48 * 3600
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def name_key(value):
    return ' '.join(sorted(norm(value).split()))


def birthday(value):
    try:
        result = date.fromisoformat(value)
        return result.isoformat() if result < date.today() else None
    except (TypeError, ValueError):
        return None


def same_type(left, right):
    # Missing type is not evidence of compatibility.
    return bool(norm(left.get('type'))) and norm(left.get('type')) == norm(right.get('type'))


def unique_rows(rows, label):
    if not isinstance(rows, list) or any(not isinstance(r, dict) or not r.get('id') for r in rows):
        raise ValueError(f'{label} must be a list of rows with IDs')
    result = {str(r['id']): r for r in rows}
    if len(result) != len(rows):
        raise ValueError(f'Duplicate {label} row IDs; snapshot may be incomplete or incorrectly paginated')
    return result


def build_report(snapshot, details):
    contacts = unique_rows(snapshot.get('contacts'), 'contacts')
    accounts = unique_rows(snapshot.get('accounts'), 'accounts')
    links = unique_rows(snapshot.get('account_contacts'), 'account_contacts')
    if not isinstance(details, list) or any(not isinstance(d, dict) or not isinstance(d.get('account'), dict) for d in details):
        raise ValueError('IBKR input must be the account-details list, not an outcome/summary report')
    indexed = defaultdict(list)
    coverage = Counter()
    for detail in details:
        a = detail['account']
        key = (identifier(a.get('masterAccountId')), identifier(a.get('accountId')))
        if not all(key):
            coverage['ibkr_details_missing_account_identity'] += 1
            continue
        indexed[key].append(detail)
    for account in accounts.values():
        key = (identifier(account.get('master_account')), identifier(account.get('ibkr_account_number')))
        coverage['accounts_with_broker_number'] += bool(key[1])
        if key[1] and not key[0]:
            coverage['accounts_missing_master'] += 1
        if all(key) and key not in indexed:
            coverage['accounts_without_ibkr_details'] += 1
    profiles = {cid: {'contact': c, 'accounts': set(), 'holders': set(), 'birthdays': set(),
                       'documents': defaultdict(set), 'sources': [], 'issues': []} for cid, c in contacts.items()}
    for link in links.values():
        cid, aid = str(link.get('contact_id')), str(link.get('account_id'))
        if cid not in contacts or aid not in accounts:
            coverage['orphan_links'] += 1
            continue
        p, account = profiles[cid], accounts[aid]
        p['accounts'].add(aid)
        if norm(p['contact'].get('type')) == 'company':
            if meaningful_id(link.get('entity_id')):
                coverage['company_links_with_person_entity_id'] += 1
                p['issues'].append({'account_id': aid, 'reason': 'company_has_person_entity_id'})
            # A company remains an account contact but never inherits its
            # representative's associatedPersons identity or date of birth.
            continue
        key = (identifier(account.get('master_account')), identifier(account.get('ibkr_account_number')))
        if not all(key):
            coverage['links_missing_account_identity'] += 1
            continue
        records = indexed.get(key, [])
        if len(records) != 1:
            coverage['links_missing_details' if not records else 'links_ambiguous_details'] += 1
            continue
        detail = records[0]
        people = detail.get('associatedPersons') or []
        if not isinstance(people, list) or any(not isinstance(person, dict) for person in people):
            raise ValueError('Malformed associatedPersons')
        entity, external = meaningful_id(link.get('entity_id')), meaningful_id(link.get('external_id'))
        # Both populated link identifiers must identify the same person. Never
        # attach the account's general email/identity to every joint holder.
        if entity or external:
            matches = [person for person in people
                       if (not entity or meaningful_id(person.get('entityId')) == entity)
                       and (not external or meaningful_id(person.get('externalCode')) == external)]
            method = 'link_identifiers'
        else:
            matches = [person for person in people if name_key(' '.join(str(person.get(k) or '') for k in ('firstName', 'middleName', 'lastName'))) == name_key(p['contact'].get('name')) and name_key(p['contact'].get('name'))]
            method = 'unique_account_name_only'
        if len(matches) != 1:
            coverage['unresolved_holder_links'] += 1
            if entity or external:
                p['issues'].append({'account_id': aid, 'reason': 'link_holder_not_uniquely_resolved'})
            continue
        person = matches[0]
        stamp = detail.get('_agm_refresh') or {}
        source = {'account_id': aid, 'broker_account': key[1], 'master_account': key[0],
                  'method': method, 'refresh_status': stamp.get('status', 'unknown'),
                  'last_success_at': stamp.get('last_success_at'), 'roles': person.get('associations', []),
                  'entity_id': person.get('entityId'), 'external_id': person.get('externalCode'),
                  'person_name': ' '.join(str(person.get(k) or '') for k in ('firstName', 'middleName', 'lastName')).strip()}
        p['sources'].append(source)
        coverage['resolved_holder_links'] += 1
        coverage['name_only_holder_links'] += method != 'link_identifiers'
        coverage['stale_or_unknown_holder_links'] += stamp.get('status') != 'fresh'
        coverage['old_or_undated_resolved_holder_links'] += not recent_source(source)
        # Name-only resolution is shown as a lead, never used to corroborate
        # the same name with its own inferred birthday/government ID.
        if method != 'link_identifiers':
            continue
        person_id = meaningful_id(person.get('entityId'))
        external_id = meaningful_id(person.get('externalCode'))
        if person_id:
            p['holders'].add((*key, 'entity', person_id))
        if external_id:
            p['holders'].add((*key, 'external', external_id))
        dob = birthday(person.get('dateOfBirth'))
        if dob:
            p['birthdays'].add(dob)
        documents = person.get('identityDocuments') or []
        if not isinstance(documents, list) or any(not isinstance(d, dict) for d in documents):
            raise ValueError('Malformed identityDocuments')
        for doc in documents:
            kind, country, number = identifier(doc.get('name')), identifier(doc.get('country')), meaningful_id(doc.get('id'))
            # Document numbers are compared only within their type + country.
            if kind and country and number:
                p['documents'][(kind, country)].add(number)
    candidates, identity_anomalies = [], []
    for left_id, right_id in combinations(sorted(profiles), 2):
        l, r = profiles[left_id], profiles[right_id]
        lc, rc = l['contact'], r['contact']
        ln, rn = name_key(lc.get('name')), name_key(rc.get('name'))
        similarity = SequenceMatcher(None, ln, rn, autojunk=False).ratio() if ln and rn else 0
        lt, rt = set(ln.split()), set(rn.split())
        left_first = norm(lc.get('name')).split()[:1]
        right_first = norm(rc.get('name')).split()[:1]
        same_first = bool(left_first) and left_first == right_first
        enough_tokens = min(len(lt), len(rt)) >= 2
        numbers_match = {t for t in lt if t.isdigit()} == {t for t in rt if t.isdigit()}
        evidence, conflicts = [], []
        if enough_tokens and ln == rn:
            evidence.append('same_normalized_name')
        elif numbers_match and enough_tokens and same_first and (lt <= rt or rt <= lt):
            evidence.append('compatible_abbreviated_name')
        elif numbers_match and same_first and len(lt & rt) >= 2 and similarity >= .90:
            evidence.append('similar_name')
        if l['holders'] & r['holders']:
            evidence.append('same_account_scoped_ibkr_person')
        # IBKR entityId identifies a person across accounts. Keep external IDs
        # account-scoped; do not infer person identity from a shared company link.
        shared_entities = ({h[-1] for h in l['holders'] if h[2] == 'entity'}
                           & {h[-1] for h in r['holders'] if h[2] == 'entity'})
        if shared_entities:
            evidence.append('same_ibkr_entity_across_accounts')
        common_docs = l['documents'].keys() & r['documents'].keys()
        if any(l['documents'][k] & r['documents'][k] for k in common_docs):
            evidence.append('same_government_id_type_country')
        if l['birthdays'] & r['birthdays']:
            evidence.append('same_ibkr_birthday')
        email_l, email_r = (str(c.get('email') or '').strip().casefold() for c in (lc, rc))
        if email_l and email_l == email_r:
            evidence.append('same_email_supporting_only')
        phones = [re.sub(r'\D', '', str(c.get('phone') or '')) for c in (lc, rc)]
        if len(phones[0]) >= 7 and phones[0] == phones[1]:
            evidence.append('same_phone_supporting_only')
        shared_accounts = l['accounts'] & r['accounts']
        if shared_accounts:
            evidence.append('shared_account_context_only')
        # Phone/email/account membership only support an independently found
        # identity/name lead. They must never generate review pairs themselves.
        triggers = {'same_normalized_name', 'compatible_abbreviated_name', 'similar_name',
                    'same_account_scoped_ibkr_person', 'same_ibkr_entity_across_accounts', 'same_government_id_type_country'}
        if not triggers.intersection(evidence):
            continue
        if norm(lc.get('type')) and norm(rc.get('type')) and not same_type(lc, rc):
            conflicts.append('different_contact_types')
        if len(l['birthdays'] | r['birthdays']) > 1:
            conflicts.append('different_ibkr_birthdays')
        if any(len(l['documents'].get(k, set()) | r['documents'].get(k, set())) > 1 for k in l['documents'].keys() | r['documents'].keys()):
            # Renewed passports/IDs may explain this; require human review.
            conflicts.append('multiple_government_ids_same_type_country')
        for aid in shared_accounts:
            for kind in ('entity', 'external'):
                lh = {h[-1] for h in l['holders'] if h[1] == identifier(accounts[aid].get('ibkr_account_number')) and h[2] == kind}
                rh = {h[-1] for h in r['holders'] if h[1] == identifier(accounts[aid].get('ibkr_account_number')) and h[2] == kind}
                if lh and rh and lh.isdisjoint(rh):
                    conflicts.append(f'different_same_account_{kind}_ids')
        if l['issues'] or r['issues']:
            conflicts.append('unresolved_link_identifiers')
        strong = bool({'same_account_scoped_ibkr_person', 'same_ibkr_entity_across_accounts', 'same_government_id_type_country'}.intersection(evidence))
        name_support = bool({'same_normalized_name', 'compatible_abbreviated_name', 'similar_name'}.intersection(evidence))
        current = all(recent_source(s) for p in (l, r) for s in p['sources'] if s['method'] == 'link_identifiers')
        if strong and not current:
            evidence.append('old_or_undated_ibkr_evidence')
        classification = 'conflicting_evidence' if conflicts else 'strong_candidate' if strong and name_support and same_type(lc, rc) and current else 'manual_review'
        destination = identity_anomalies if strong and not name_support else candidates
        destination.append({'left_contact_id': left_id, 'right_contact_id': right_id,
                           'classification': classification, 'name_similarity': round(similarity, 4),
                           'evidence': evidence, 'conflicts': sorted(set(conflicts)),
                           'shared_ibkr_entity_ids': sorted(shared_entities),
                           'shared_account_ids': sorted(shared_accounts)})
    coverage['contacts_without_links'] = sum(not p['accounts'] for p in profiles.values())
    coverage['contacts_without_resolved_identity'] = sum(not p['holders'] for p in profiles.values())
    coverage['contacts_with_multiple_birthdays'] = sum(len(p['birthdays']) > 1 for p in profiles.values())
    coverage['contacts_with_government_ids'] = sum(bool(p['documents']) for p in profiles.values())
    coverage['duplicate_ibkr_account_keys'] = sum(len(v) > 1 for v in indexed.values())
    return {'schema_version': 1, 'generated_at': datetime.now(timezone.utc).isoformat(),
            'database_captured_at': snapshot.get('captured_at'),
            'counts': {'contacts': len(contacts), 'accounts': len(accounts), 'account_contacts': len(links),
                       'ibkr_detail_accounts': len(details), 'candidate_pairs': len(candidates),
                       'identity_link_anomalies': len(identity_anomalies),
                       **{kind: sum(c['classification'] == kind for c in candidates)
                          for kind in ('strong_candidate', 'manual_review', 'conflicting_evidence')}},
            'coverage': dict(coverage),
            'limitations': ['Candidate pairs are review leads, never merge instructions or transitive clusters.',
                           'IBKR details are a supplied snapshot; fresh means its recorded refresh status, not current provider verification.',
                           'No local government-ID/birthday columns exist; unlinked contacts cannot acquire those signals through this join.',
                           'Name-only holder matches do not contribute birthday or government-ID corroboration.',
                           'No document, screening, user or advisor dependency merge plan is produced.'],
            'candidates': candidates,
            'identity_link_anomalies': identity_anomalies,
            'contacts': [{**p['contact'], 'account_ids': sorted(p['accounts']),
                          'resolved_birthdays': sorted(p['birthdays']),
                          'resolved_government_ids': [{'type': kind, 'country': country, 'numbers': sorted(numbers)}
                                                     for (kind, country), numbers in sorted(p['documents'].items())],
                          'ibkr_sources': p['sources'], 'link_issues': p['issues']} for p in profiles.values()]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True, help='DB contacts/accounts/account_contacts JSON exported read-only')
    parser.add_argument('--ibkr-details', type=Path, required=True, help='Existing account-details list; never calls IBKR')
    parser.add_argument('--output', type=Path, required=True, help='New private JSON report (refuses overwrite)')
    args = parser.parse_args()
    try:
        snapshot_bytes, details_bytes = args.snapshot.read_bytes(), args.ibkr_details.read_bytes()
        report = build_report(json.loads(snapshot_bytes), json.loads(details_bytes))
        report['input_sha256'] = {'database': hashlib.sha256(snapshot_bytes).hexdigest(), 'ibkr_details': hashlib.sha256(details_bytes).hexdigest()}
        with os.fdopen(os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
            f.write('\n')
        print(json.dumps({'counts': report['counts'], 'coverage': report['coverage']}, sort_keys=True))
    except (ValueError, OSError) as exc:
        # Do not print input content or exception messages containing PII.
        reason = str(exc) if isinstance(exc, ValueError) and not isinstance(exc, json.JSONDecodeError) else type(exc).__name__
        parser.exit(1, f'Report failed ({reason}); verify inputs and use a new output path.\n')


if __name__ == '__main__':
    main()
