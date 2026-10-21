"""Account-opening identity matching. All writes commit with the account links.

The endpoint reads the saved application; clients never choose a contact ID or
claim that applicant-supplied identity was verified. No provider calls occur in
this transaction and generic contact reads never include the private tables.
"""
import hashlib
import re
import unicodedata
import uuid
from datetime import date, datetime

from sqlalchemy import func, or_, select, text
from sqlalchemy.exc import SQLAlchemyError

from src.utils.exception import ServiceError


def name_key(value):
    value = unicodedata.normalize('NFKD', str(value or '')).casefold()
    return ' '.join(''.join(c if c.isalnum() else ' ' for c in value if not unicodedata.combining(c)).split())


def fail(outcome, message):
    raise ServiceError(message, 409, 'contact_identity_' + outcome, {'outcome': outcome})


def person_data(holder):
    if not isinstance(holder, dict) or not isinstance(holder.get('name'), dict):
        raise ServiceError('Holder identity is malformed.', 422, 'contact_identity_invalid')
    name = holder.get('name') or {}
    result = {'name': ' '.join(str(name.get(k) or '').strip() for k in ('first', 'last')).strip(),
              'email': str(holder.get('email') or '').strip() or None, 'type': 'person',
              'external_id': str(holder.get('externalId') or '').strip() or None,
              'entity_id': holder.get('entityId'), 'birthday': None, 'documents': []}
    birth = holder.get('dateOfBirth')
    if birth:
        try:
            result['birthday'] = date.fromisoformat(str(birth))
        except (ValueError, TypeError):
            raise ServiceError('A valid holder date of birth is required.', 422, 'contact_identity_invalid') from None
        if result['birthday'] >= date.today():
            raise ServiceError('Holder date of birth must be in the past.', 422, 'contact_identity_invalid')
    identification = holder.get('identification') or {}
    country = str(identification.get('issuingCountry') or '').strip().upper()
    for field in ('passport', 'nationalCard', 'driversLicense'):
        raw = identification.get(field)
        if raw:
            if not isinstance(raw, str):
                raise ServiceError('Identification numbers must be text.', 422, 'contact_identity_invalid')
            number = re.sub(r'[\s.\-]', '', raw).upper()
            if not re.fullmatch(r'[A-Z0-9]{1,128}', number) or not re.fullmatch(r'[A-Z]{3}', country):
                raise ServiceError('Identification requires a supported number and issuing country.', 422, 'contact_identity_invalid')
            result['documents'].append((field, country, number))
    if not result['name']:
        raise ServiceError('Holder name is required.', 422, 'contact_identity_invalid')
    if result['entity_id'] is not None:
        try:
            if isinstance(result['entity_id'], bool):
                raise ValueError()
            result['entity_id'] = int(str(result['entity_id']))
            if not 0 < result['entity_id'] < 2**63:
                raise ValueError()
        except (ValueError, TypeError):
            raise ServiceError('Invalid holder entity identifier.', 422, 'contact_identity_invalid') from None
    return result


def applicants(application):
    if not isinstance(application, dict) or not isinstance(application.get('customer'), dict):
        raise ServiceError('A saved application customer is required.', 422, 'contact_identity_invalid')
    customer = application['customer']
    kind = customer.get('type')
    if kind == 'INDIVIDUAL':
        holders = (customer.get('accountHolder') or {}).get('accountHolderDetails') or []
        if len(holders) != 1:
            raise ServiceError('One individual holder is required.', 422, 'contact_identity_invalid')
        return [person_data(holders[0])]
    if kind == 'JOINT':
        joint = customer.get('jointHolders') or {}
        first, second = joint.get('firstHolderDetails') or [], joint.get('secondHolderDetails') or []
        if len(first) != 1 or len(second) != 1:
            raise ServiceError('Both joint holders are required.', 422, 'contact_identity_invalid')
        return [person_data(first[0]), person_data(second[0])]
    if kind == 'ORG':
        org = customer.get('organization') or {}
        company = str(((org.get('identifications') or [{}])[0]).get('name') or '').strip()
        holders = (org.get('associatedEntities') or {}).get('associatedIndividuals') or []
        if not company or not holders:
            raise ServiceError('A company and its associated people are required.', 422, 'contact_identity_invalid')
        return [{'name': company, 'email': None, 'type': 'company', 'external_id': customer.get('externalId'),
                 'entity_id': None, 'birthday': None, 'documents': []}] + [person_data(h) for h in holders]
    raise ServiceError('Unsupported application customer type.', 422, 'contact_identity_invalid')


def linked_person(link, application):
    """Only durable person IDs resolve legacy holder evidence; name is not proof."""
    try:
        people = [p for p in applicants(application) if p['type'] == 'person']
    except ServiceError:
        return None
    if link['entity_id'] is None and not link['external_id']:
        return None
    matches = [p for p in people if
               (link['entity_id'] is None or p['entity_id'] == link['entity_id']) and
               (not link['external_id'] or p['external_id'] == link['external_id'])]
    return matches[0] if len(matches) == 1 else None


def reconcile_application_contacts(db, account_id):
    try:
        account_id = uuid.UUID(str(account_id))
    except (ValueError, TypeError, AttributeError):
        raise ServiceError('A valid account_id is required.', 422, 'contact_identity_invalid') from None
    try:
        return _reconcile(db, account_id)
    except SQLAlchemyError:
        # SQLAlchemy errors can embed parameter values, including identity data.
        raise ServiceError('Contact setup is temporarily unavailable.', 503, 'contact_identity_unavailable') from None


def _reconcile(db, account_id):
    contacts, accounts, links, identities, documents = [db.metadata.tables[t] for t in
        ('contact', 'account', 'account_contact', 'contact_identity', 'contact_identity_document')]
    stamp = datetime.now().strftime('%Y%m%d%H%M%S')
    result = []
    with db.engine.begin() as conn:
        # Short critical section: serializes identity creation across applications.
        # It includes no screening, document upload, browser work or broker calls.
        key = int.from_bytes(hashlib.sha256(b'application-contact-identity-v1').digest()[:8], 'big', signed=True)
        conn.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': key})
        account = conn.execute(select(accounts).where(accounts.c.id == account_id).with_for_update()).mappings().one_or_none()
        if not account:
            raise ServiceError('Application account not found.', 404, 'contact_identity_account_missing')
        if account['ibkr_account_number']:
            raise ServiceError('Contact setup is limited to pending applications.', 409, 'contact_identity_not_pending')
        people = applicants(account['application_json'])
        account_links = conn.execute(select(links).where(links.c.account_id == account_id)).mappings().all()
        chosen = set()
        for person in people:
            scoped = [(documents.c.document_type == t) & (documents.c.issuing_country == c) &
                      (documents.c.identifier == n) for t, c, n in person['documents']]
            owner_ids = set(conn.execute(select(documents.c.contact_id).where(or_(*scoped))).scalars()) if scoped else set()
            # An existing exact link is additional identity evidence, not an email fallback.
            linked_ids = {l['contact_id'] for l in account_links if
                ((person['entity_id'] is not None or person['external_id']) and
                 (person['entity_id'] is None or l['entity_id'] == person['entity_id']) and
                 (not person['external_id'] or l['external_id'] == person['external_id']))}
            if len(owner_ids | linked_ids) > 1:
                fail('conflict', 'Identity details point to different contacts. AGM review is required.')
            contact_id = next(iter(owner_ids | linked_ids), None)
            if person['type'] == 'company':
                company_ids = set(conn.execute(select(contacts.c.id).where(
                    contacts.c.id.in_([l['contact_id'] for l in account_links]),
                    contacts.c.type == 'company')).scalars())
                named = [c for c in conn.execute(select(contacts).where(contacts.c.id.in_(company_ids))).mappings()
                         if name_key(c['name']) == name_key(person['name'])]
                if len(named) > 1:
                    fail('ambiguous', 'Company contact is ambiguous. AGM review is required.')
                contact_id = contact_id or (named[0]['id'] if named else None)
            elif not person['birthday'] or not person['documents']:
                fail('insufficient', 'A holder birthday and government identification are required for contact setup.')
            if not contact_id and person['type'] == 'person':
                contact_id = _legacy_match(conn, contacts, links, accounts, identities, person, account_id)
            created = contact_id is None
            if created:
                contact_id = uuid.uuid4()
                conn.execute(contacts.insert().values(id=contact_id, name=person['name'], email=person['email'],
                                                     type=person['type'], created=stamp, updated=stamp))
            else:
                contact = conn.execute(select(contacts).where(contacts.c.id == contact_id).with_for_update()).mappings().one()
                if contact['type'] and contact['type'] != person['type']:
                    fail('conflict', 'Company and person identity cannot be combined. AGM review is required.')
                if name_key(contact['name']) != name_key(person['name']):
                    fail('conflict', 'Contact name disagrees with the linked identity. AGM review is required.')
                if not contact['type']:
                    conn.execute(contacts.update().where(contacts.c.id == contact_id).values(type=person['type'], updated=stamp))
            if contact_id in chosen:
                fail('conflict', 'Different application holders resolve to the same contact. AGM review is required.')
            chosen.add(contact_id)
            if person['type'] == 'person':
                identity = None if created else conn.execute(select(identities).where(identities.c.contact_id == contact_id)).mappings().one_or_none()
                if not created and not identity:
                    _check_legacy_evidence(conn, links, accounts, contact_id, person, account_id)
                _save_identity(conn, identities, documents, contact_id, person, account_id, stamp, identity)
            own_links = [l for l in account_links if l['contact_id'] == contact_id]
            compatible = [l for l in own_links if
                (person['type'] == 'company' or person['entity_id'] is None or l['entity_id'] in (None, person['entity_id'])) and
                (not person['external_id'] or l['external_id'] in (None, '', person['external_id']))]
            if own_links and not compatible:
                fail('conflict', 'Existing account link has conflicting holder identifiers. AGM review is required.')
            if compatible:
                for link in compatible:
                    patch = {}
                    if person['type'] == 'company' and link['entity_id'] is not None:
                        patch['entity_id'] = None
                    elif link['entity_id'] is None and person['entity_id'] is not None:
                        patch['entity_id'] = person['entity_id']
                    if not link['external_id'] and person['external_id']:
                        patch['external_id'] = person['external_id']
                    if patch:
                        conn.execute(links.update().where(links.c.id == link['id']).values(**patch, updated=stamp))
            else:
                conn.execute(links.insert().values(id=uuid.uuid4(), account_id=account_id, contact_id=contact_id,
                    entity_id=person['entity_id'], external_id=person['external_id'], created=stamp, updated=stamp))
            result.append({'name': person['name'], 'contact_id': str(contact_id), 'type': person['type'],
                           'entityId': str(person['entity_id']) if person['entity_id'] is not None else None,
                           'externalId': person['external_id'], 'outcome': 'created' if created else 'matched'})
    return {'linked_contacts': result}


def _legacy_match(conn, contacts, links, accounts, identities, person, current_account_id):
    # Legacy contacts have no private identity yet. Examine only plausible rows,
    # then corroborate via independently ID-resolved holders on linked accounts.
    candidates = conn.execute(select(contacts).where(or_(
        func.lower(func.trim(contacts.c.email)) == (person['email'] or '').lower(),
        func.lower(func.trim(contacts.c.name)) == person['name'].lower()))).mappings().all()
    matches, unresolved = set(), False
    supplied = set(person['documents'])
    for candidate in candidates:
        if candidate['type'] and candidate['type'] != 'person':
            continue
        private = conn.execute(select(identities).where(identities.c.contact_id == candidate['id'])).mappings().one_or_none()
        if private:
            # No supplied document matched this contact. Same birthday/name can
            # mean a replacement document, and requires an explicit review.
            if private['date_of_birth'] == person['birthday'] and name_key(candidate['name']) == name_key(person['name']):
                unresolved = True
            continue
        evidence = conn.execute(select(links, accounts.c.application_json).join(accounts, accounts.c.id == links.c.account_id)
            .where(links.c.contact_id == candidate['id'], accounts.c.id != current_account_id)).mappings().all()
        established = False
        for link in evidence:
            holder = linked_person(link, link['application_json'])
            if not holder or not holder['birthday'] or not holder['documents']:
                continue
            established = True
            if supplied.intersection(holder['documents']):
                if holder['birthday'] != person['birthday']:
                    fail('conflict', 'Government identification and birthday disagree. AGM review is required.')
                matches.add(candidate['id'])
            elif holder['birthday'] == person['birthday'] and name_key(candidate['name']) == name_key(person['name']):
                unresolved = True
        if not established and name_key(candidate['name']) == name_key(person['name']):
            unresolved = True
    if len(matches) > 1:
        fail('ambiguous', 'More than one contact has this identity. AGM review is required.')
    if matches:
        return next(iter(matches))
    if unresolved:
        fail('insufficient', 'An existing contact needs identity review before it can be linked. Your application is saved.')
    return None


def _check_legacy_evidence(conn, links, accounts, contact_id, person, current_account_id):
    evidence = conn.execute(select(links, accounts.c.application_json).join(accounts, accounts.c.id == links.c.account_id)
        .where(links.c.contact_id == contact_id, accounts.c.id != current_account_id)).mappings().all()
    supplied = set(person['documents'])
    for link in evidence:
        holder = linked_person(link, link['application_json'])
        if not holder:
            continue
        if holder['birthday'] and holder['birthday'] != person['birthday']:
            fail('conflict', 'Linked holder birthdays disagree. AGM review is required.')
        known = set(holder['documents'])
        if known and not known.intersection(supplied) and any((t, c) in {(kt, kc) for kt, kc, _ in known} for t, c, _ in supplied):
            fail('conflict', 'Linked holder identification disagrees. AGM review is required.')


def _save_identity(conn, identities, documents, contact_id, person, account_id, stamp, existing):
    if existing and existing['date_of_birth'] != person['birthday']:
        fail('conflict', 'Contact birthday disagrees with the supplied identity. AGM review is required.')
    if not existing:
        conn.execute(identities.insert().values(contact_id=contact_id, date_of_birth=person['birthday'],
            source_account_id=account_id, source='application', verification_status='applicant_declared', created=stamp, updated=stamp))
    prior_documents = conn.execute(select(documents).where(documents.c.contact_id == contact_id)).mappings().all()
    known = {(d['document_type'], d['issuing_country'], d['identifier']) for d in prior_documents}
    supplied = set(person['documents'])
    scopes = {(kind, country) for kind, country, _ in known}
    if known and not known.intersection(supplied) and any((kind, country) in scopes for kind, country, _ in supplied):
        fail('conflict', 'Identification changed without corroborating identity. AGM review is required.')
    for kind, country, number in person['documents']:
        stored = conn.execute(select(documents).where(documents.c.document_type == kind,
            documents.c.issuing_country == country, documents.c.identifier == number)).mappings().one_or_none()
        if stored and stored['contact_id'] != contact_id:
            fail('conflict', 'Government identification belongs to a different contact. AGM review is required.')
        if not stored:
            conn.execute(documents.insert().values(id=uuid.uuid4(), contact_id=contact_id,
                document_type=kind, issuing_country=country, identifier=number, source_account_id=account_id,
                source='application', verification_status='applicant_declared', created=stamp, updated=stamp))
