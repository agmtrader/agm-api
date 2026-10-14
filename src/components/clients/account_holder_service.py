"""Resolve AGM account identity and validate fresh IBKR details before maintenance."""
from src.utils.exception import ServiceError, handle_exception
from src.utils.connectors.ibkr_web_api import IBKRWebAPI
from src.components.clients.holder_updates import eligibility, person_key, select_holder, validate_changes
from uuid import UUID


def _context(account_id):
    from src.components.clients.accounts import db, ibkr_web_api
    if not isinstance(account_id, str) or not account_id.strip():
        raise ServiceError('Internal account ID is required.', 400, code='holder_update_invalid')
    try:
        UUID(account_id)
    except ValueError:
        raise ServiceError('Invalid internal account ID.', 400, code='holder_update_invalid')
    accounts = db.read(table='account', query={'id': account_id}) or []
    if len(accounts) != 1:
        raise ServiceError('Account not found.', 404, code='account_not_found')
    account = accounts[0]
    master = str(account.get('master_account') or '').strip().upper()
    broker_id = account.get('ibkr_account_number')
    if not broker_id or master not in IBKRWebAPI.MASTER_ACCOUNT_CREDENTIALS:
        raise ServiceError('A submitted account with a supported IBKR master is required.', 400, code='ibkr_master_unsupported')
    details = ibkr_web_api.get_account_details(account_id=broker_id, master_account=master)
    broker_account = details.get('account', {})
    if broker_account.get('accountId') != broker_id or broker_account.get('masterAccountId') != master:
        raise ServiceError('IBKR details do not match the stored account and master. Reconcile the account before editing.', 409, code='ibkr_account_mismatch')
    tasks = None
    if broker_account.get('clearingStatus') in {'N', 'P'}:
        tasks = ibkr_web_api.get_pending_tasks(account_id=broker_id, master_account=master)
    def translated(node):
        if isinstance(node, list):
            return any(translated(child) for child in node)
        if isinstance(node, dict):
            return node.get('translation') is True or node.get('translated') is True or any(translated(child) for child in node.values())
        return False
    reason = eligibility(details, tasks)
    if translated(account.get('application_json')) or translated(details):
        reason = 'Dual-language changes require native and translated details in IBKR Portal.'
    return account, details, reason


@handle_exception
def get_account_holder_edit_context(account_id):
    account, details, reason = _context(account_id)
    fields = {'entityId', 'externalCode', 'firstName', 'middleName', 'lastName', 'salutation', 'suffix',
              'dateOfBirth', 'countryOfBirth', 'residence', 'mailing', 'phones', 'associations', 'translation', 'translated'}
    return {'eligible': reason is None, 'reason': reason, 'account_id': account['ibkr_account_number'],
            'people': [{**{k: v for k, v in p.items() if k in fields}, 'person_key': person_key(p)}
                       for p in details.get('associatedPersons', [])]}


@handle_exception
def change_account_holder_detail(account_id, key, changes):
    from src.components.clients.accounts import ibkr_web_api
    validate_changes(changes)
    account, details, reason = _context(account_id)
    if reason:
        raise ServiceError(reason, 409, code='ibkr_holder_update_unavailable')
    person = select_holder(details, key)
    # Compare against current broker details, rather than trusting a client no-op check.
    current = {'name': {k: person.get(v, '') for k, v in {'first': 'firstName', 'middle': 'middleName', 'last': 'lastName', 'salutation': 'salutation', 'suffix': 'suffix'}.items()},
               'dateOfBirth': person.get('dateOfBirth'), 'countryOfBirth': person.get('countryOfBirth'),
               'residenceAddress': person.get('residence') or {}, 'mailingAddress': person.get('mailing') or {}}
    if 'phones' not in changes and all(
        all(current[field].get(k, '') == v for k, v in change.items()) if isinstance(change, dict)
        else current.get(field) == change for field, change in changes.items()
    ):
        raise ServiceError('No profile changes were supplied.', 400, code='holder_update_invalid')
    identity = {'id': str(person['entityId'])} if person.get('entityId') else {'externalId': str(person['externalCode'])}
    return ibkr_web_api.change_account_holder_detail(
        account_id=account['ibkr_account_number'], master_account=account['master_account'].strip().upper(),
        holder_details={**identity, **changes},
    )
