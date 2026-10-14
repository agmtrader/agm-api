"""Account-scoped holder maintenance: validate before any broker write."""
from datetime import date
import re
from src.utils.exception import ServiceError


def invalid(message):
    raise ServiceError(message, status_code=400, code='holder_update_invalid')


def person_key(person):
    entity = str(person.get('entityId') or '').strip()
    external = str(person.get('externalCode') or '').strip()
    return f'entity:{entity}' if entity else f'external:{external}' if external else None


def eligibility(details, tasks=None):
    account = details.get('account') or {}
    if str(account.get('applicantType') or '').strip().upper() not in {'INDIVIDUAL', 'JOINT'}:
        return 'IBKR supports profile updates only for Individual and Joint accounts.'
    status = account.get('clearingStatus')
    if status == 'O':
        return None
    if status in {'N', 'P'}:
        pending = (tasks or {}).get('pendingTasks', [])
        if any(str(t.get('formNumber')) == '9974' for t in pending if isinstance(t, dict)):
            return None
        return 'Pending accounts require IBKR registration task 9974 to update holder details.'
    return 'This account must be open, or pending with IBKR task 9974.'


def validate_changes(changes):
    if not isinstance(changes, dict) or not changes:
        invalid('At least one changed profile field is required.')
    allowed = {'name', 'dateOfBirth', 'countryOfBirth', 'residenceAddress', 'mailingAddress', 'phones'}
    if changes.keys() - allowed:
        invalid('Unsupported profile field. Login email requires IBKR updateCredentials.')

    def string(value, label, maximum, blank=False):
        if not isinstance(value, str) or len(value) > maximum or (not blank and not value.strip()):
            invalid(f'{label} is missing or invalid.')

    def country(value, label):
        if not isinstance(value, str) or not re.fullmatch(r'[A-Z]{3}', value):
            invalid(f'{label} must be a three-letter country code.')

    if 'name' in changes:
        name = changes['name']
        if not isinstance(name, dict) or name.keys() - {'first', 'middle', 'last', 'salutation', 'suffix'}:
            invalid('Invalid legal name fields.')
        for key in ('first', 'last'):
            string(name.get(key), f'Legal name {key}', 64)
        for key in name.keys() - {'first', 'last'}:
            string(name[key], f'Legal name {key}', 64, blank=True)
    if 'dateOfBirth' in changes:
        raw = changes['dateOfBirth']
        try:
            dob = date.fromisoformat(raw)
            if dob.isoformat() != raw or dob >= date.today():
                invalid('Date of birth must be a past date in YYYY-MM-DD format.')
        except (TypeError, ValueError):
            invalid('Date of birth must be a past date in YYYY-MM-DD format.')
    if 'countryOfBirth' in changes:
        country(changes['countryOfBirth'], 'Country of birth')
    for field in ('residenceAddress', 'mailingAddress'):
        if field not in changes:
            continue
        address = changes[field]
        lengths = {'street1': 200, 'street2': 200, 'city': 100, 'state': 40, 'postalCode': 20}
        if not isinstance(address, dict) or address.keys() - (lengths.keys() | {'country'}):
            invalid('Invalid address fields.')
        country(address.get('country'), 'Address country')
        for key in ('street1', 'city', 'postalCode'):
            string(address.get(key), key, lengths[key])
        for key in address.keys() - {'country', 'street1', 'city', 'postalCode'}:
            string(address[key], key, lengths[key], blank=True)
    if 'phones' in changes:
        phones = changes['phones']
        if not isinstance(phones, list) or not 1 <= len(phones) <= 5:
            invalid('Provide between one and five phone numbers.')
        types = set()
        for phone in phones:
            if not isinstance(phone, dict) or phone.keys() - {'type', 'number', 'country'}:
                invalid('Invalid phone fields.')
            kind = phone.get('type')
            if not isinstance(kind, str) or kind not in {'Mobile', 'Home', 'Work', 'Fax', 'Business'} or kind in types:
                invalid('Phone types must be supported and unique.')
            types.add(kind)
            string(phone.get('number'), 'Phone number', 18)
            if not re.fullmatch(r'[+0-9 ()-]{3,18}', phone['number']):
                invalid('Invalid phone number.')
            country(phone.get('country'), 'Phone country')
        if 'Mobile' not in types:
            invalid('A mobile phone number is required.')
    return changes


def select_holder(details, key):
    people = details.get('associatedPersons') or []
    matches = [p for p in people if person_key(p) == key]
    if len(matches) != 1:
        invalid('The selected individual no longer matches the IBKR account. Refresh the page.')
    person = matches[0]
    # Trusted contacts are displayed beside holders but are not brokerage applicants.
    roles = {re.sub(r'[^a-z]', '', str(role).lower()) for role in person.get('associations', [])}
    holder_roles = {'accountholder', 'firstholder', 'secondholder', 'primaryholder', 'secondaryholder', 'firstaccountholder', 'secondaccountholder', 'primaryaccountholder', 'secondaryaccountholder'}
    if not roles.intersection(holder_roles):
        invalid('Only IBKR account holders can be updated through this operation.')
    if person.get('translation') is True or person.get('translated') is True:
        invalid('Dual-language holder changes require both native and translated details in IBKR Portal.')
    return person


def parse_update_result(data, account_id):
    """IBKR can return ERROR inside HTTP 200; do not mistake transport for acceptance."""
    try:
        results = data['fileData']['data']['changeAccountHolderDetail']
    except (KeyError, TypeError):
        results = None
    if isinstance(results, dict):
        results = [results]
    if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
        raise ServiceError('IBKR returned an unrecognized holder-update result. Verify in IBKR Portal before resubmitting.', 502, code='ibkr_holder_result_unknown')
    result = results[0]
    if result.get('referenceAccountId') != account_id:
        raise ServiceError('IBKR returned an unexpected account in the update result. Verify in IBKR Portal.', 502, code='ibkr_holder_result_unknown')
    status = str(result.get('status') or '').upper()
    if result.get('error') or status in {'ERROR', 'REJECTED', 'FAILED'}:
        errors = result.get('error')
        messages = [e['value'][:500] for e in errors if isinstance(e, dict) and isinstance(e.get('value'), str)] if isinstance(errors, list) else []
        raise ServiceError('IBKR rejected the holder update.' + (' ' + ' '.join(messages[:3]) if messages else ''), 422, code='ibkr_holder_update_rejected')
    if status not in {'PENDING', 'PROCESSED', 'SUCCESS', 'COMPLETED', 'ACCEPTED', 'SUBMITTED'}:
        raise ServiceError('IBKR update status is unknown. Verify in IBKR Portal before resubmitting.', 502, code='ibkr_holder_result_unknown')
    return {'status': status, 'request_id': result.get('requestId') or data.get('requestId'),
            'message': 'Request accepted by IBKR. Refresh details and review registration tasks to verify the resulting profile.'}
