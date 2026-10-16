"""Durable outbound transfer workflow. Never retry a broker submission automatically."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re
from uuid import UUID
from zoneinfo import ZoneInfo

from flask import g
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.components.clients import accounts
from src.utils.exception import ServiceError


def now():
    return datetime.now(timezone.utc).isoformat()


def identifier(value, label):
    try:
        return str(UUID(str(value)))
    except (ValueError, TypeError):
        raise ServiceError(f'{label} must be a UUID', 400, code='validation_error')


def text(value, label, maximum=128):
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > maximum or not value.isascii() or any(ord(c) < 32 for c in value):
        raise ServiceError(f'{label} is required and must contain at most {maximum} ASCII characters', 400, code='validation_error')
    return value.strip()


def table():
    return accounts.db.base.metadata.tables['external_asset_transfer']


def account_context(account_id):
    account_id = identifier(account_id, 'account_id')
    rows = accounts.db.read(table='account', query={'id': account_id})
    if len(rows) != 1:
        raise ServiceError('Account not found', 404)
    account = rows[0]
    master = account.get('master_account')
    if master not in accounts.IBKRWebAPI.MASTER_ACCOUNT_CREDENTIALS:
        raise ServiceError('Unsupported IBKR master account', 400, code='ibkr_master_unsupported')
    if not account.get('ibkr_account_number'):
        raise ServiceError('Account has no IBKR account number', 400)
    return account


def public_record(row):
    result = dict(row)
    for key in ('id', 'account_id', 'client_instruction_id'):
        result[key] = str(result[key])
    result['settlement_verified'] = result.get('clearing_state') == 'SETTLED'
    result['follow_up_ready'] = bool(result.get('instruction_id') and result.get('message_center_ticket_reference'))
    return result


def read_record(transfer_id, account_id):
    transfer_id = identifier(transfer_id, 'transfer_id')
    account_id = identifier(account_id, 'account_id')
    t = table()
    with accounts.db.engine.connect() as conn:
        row = conn.execute(select(t).where(t.c.id == transfer_id, t.c.account_id == account_id)).mappings().first()
    if row is None:
        raise ServiceError('Transfer not found for this account', 404)
    return dict(row)


def list_transfers(account_id):
    account_id = identifier(account_id, 'account_id')
    t = table()
    with accounts.db.engine.connect() as conn:
        rows = conn.execute(select(t).where(t.c.account_id == account_id).order_by(t.c.created.desc()).limit(100)).mappings()
        return [public_record(row) for row in rows]


def validate_request(payload, account):
    if payload.get('master_account') not in (None, account['master_account']) or payload.get('ibkr_account_id') not in (None, account['ibkr_account_number']):
        raise ServiceError('Account context does not match the stored account', 400)
    broker = payload.get('contra_broker_info')
    if not isinstance(broker, dict):
        raise ServiceError('Receiving institution details are required', 400)
    limits = {'accountType': 32, 'brokerName': 128, 'brokerAccountId': 64, 'country': 3, 'contactEmail': 64, 'contactPhone': 16}
    broker = {key: text(broker.get(key), key, limit) for key, limit in limits.items()}
    if broker['accountType'] not in ('INDIVIDUAL', 'JOINT', 'ORG', 'TRUST') or not re.fullmatch(r'[A-Z]{3}', broker['country']):
        raise ServiceError('Invalid receiving account type or ISO country', 400)
    if not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+', broker['contactEmail']) or not re.fullmatch(r'\+[1-9]\d{6,14}', broker['contactPhone']):
        raise ServiceError('A valid contact email and international phone number are required', 400)
    bic = text(payload.get('pset_bic'), 'Settlement BIC', 11).upper()
    if not re.fullmatch(r'[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?', bic):
        raise ServiceError('Settlement BIC must contain 8 or 11 characters; QA placeholders are not accepted', 400)
    currency = text(payload.get('currency'), 'currency', 3).upper()
    if not re.fullmatch(r'[A-Z]{3}', currency):
        raise ServiceError('Invalid currency', 400)
    try:
        trade = date.fromisoformat(payload.get('trade_date', ''))
        settle = date.fromisoformat(payload.get('settle_date', ''))
    except (TypeError, ValueError):
        raise ServiceError('Trade and settlement dates must use YYYY-MM-DD', 400)
    today = datetime.now(ZoneInfo('America/New_York')).date()
    if trade < today or trade > today + timedelta(days=30) or settle < trade:
        raise ServiceError('Trade date must be current or within 30 days; settlement cannot precede trade', 400)
    positions = payload.get('positions')
    if not isinstance(positions, list) or not 1 <= len(positions) <= 100:
        raise ServiceError('Select between 1 and 100 Treasury-bill positions', 400)
    normalized = []
    seen = set()
    for position in positions:
        if not isinstance(position, dict) or not re.fullmatch(r'[1-9]\d*', str(position.get('conid', ''))):
            raise ServiceError('Each position requires a valid conid', 400)
        conid = str(position['conid'])
        try:
            quantity = Decimal(str(position['quantity']))
            if not quantity.is_finite() or quantity <= 0 or quantity != quantity.to_integral_value() or quantity > 10**12 or conid in seen:
                raise ValueError()
        except (KeyError, InvalidOperation, ValueError):
            raise ServiceError('Position quantities must be positive integers with no duplicate conids', 400)
        seen.add(conid)
        normalized.append({'conid': conid, 'quantity': int(quantity)})
    return {'accountId': account['ibkr_account_number'], 'direction': 'OUT', 'instructionType': 'COMPLEX_ASSET_TRANSFER',
            'currency': currency, 'positions': normalized, 'contraBrokerInfo': broker,
            'nonDisclosedDetail': {'tradeDate': trade.isoformat(), 'settleDate': settle.isoformat(), 'psetBic': bic}}


def enrich_positions(data):
    """Recheck holdings on submission; never trust browser asset class/quantity labels."""
    holdings = accounts.read_ibkr_account_positions(data['accountId'])
    positions = {str(p.get('conid')): p for p in holdings if isinstance(p, dict)}
    for selected in data['positions']:
        p = positions.get(selected['conid'], {})
        try:
            sufficient = Decimal(str(p.get('position', 0))) >= selected['quantity']
        except InvalidOperation:
            sufficient = False
        if not sufficient or (p.get('secType') or p.get('assetClass')) != 'BILL' or p.get('currency') != data['currency']:
            raise ServiceError('Selected Treasury-bill holdings or quantities changed; reload positions', 409)
        selected['description'] = str(p.get('description') or '')
        selected['quantityUnit'] = 'IBKR position quantity'
        for key in ('cusip', 'isin', 'maturityDate'):
            if p.get(key):
                selected[key] = str(p[key])


def response_fields(response, record):
    r = response.get('instructionResult') if isinstance(response, dict) else None
    if not isinstance(r, dict) or str(r.get('clientInstructionId')) != str(record['client_instruction_id']):
        raise ServiceError('IBKR response identity could not be verified. Refresh status; do not resubmit.', 502, code='transfer_response_unverified')
    if record.get('instruction_id') and str(r.get('instructionId')) != record['instruction_id']:
        raise ServiceError('IBKR returned a different instruction identity', 502, code='transfer_response_unverified')
    if not r.get('instructionId') or not r.get('instructionStatus'):
        raise ServiceError('IBKR instruction result is incomplete. Refresh the saved request.', 502, code='transfer_response_unverified')
    fields = {}
    for source, target in (('instructionId', 'instruction_id'), ('ibReferenceId', 'ib_reference_id')):
        if r.get(source) is not None:
            if not str(r[source]).isdigit():
                raise ServiceError('Invalid IBKR instruction reference', 502)
            fields[target] = str(r[source])
    if response.get('instructionSetId') is not None:
        fields['instruction_set_id'] = str(response['instructionSetId'])
    if r.get('instructionStatus'):
        fields['instruction_status'] = str(r['instructionStatus'])
    clearing = r.get('clearingState') or r.get('clearing_state') or response.get('clearingState')
    if clearing:
        fields['clearing_state'] = str(clearing)
    if r.get('error'):
        errors = r['error'] if isinstance(r['error'], list) else [r['error']]
        error_details = [{key: str(item[key])[:500] for key in ('code', 'errorCode', 'message', 'errorMessage', 'description') if item.get(key) is not None}
                         for item in errors if isinstance(item, dict)]
        fields['last_error'] = {'code': 'ibkr_instruction_error', 'message': 'IBKR reported an instruction error; review the instruction in Portal.', 'details': error_details}
    else:
        fields['last_error'] = None
    fields['submission_state'] = 'REJECTED' if r.get('instructionStatus') == 'REJECTED' else 'SUBMITTED'
    return fields


def save_fields(record, fields):
    t = table()
    with accounts.db.engine.begin() as conn:
        row = conn.execute(t.update().where(t.c.id == record['id']).values(**fields, updated=now()).returning(t)).mappings().one()
        return public_record(row)


def submit_transfer(payload):
    if not isinstance(payload, dict):
        raise ServiceError('JSON object required', 400)
    transfer_id = identifier(payload.get('request_id'), 'request_id')
    account = account_context(payload.get('account_id'))
    t = table()
    # A retry with the same browser UUID only reads the saved result, even after a timeout.
    with accounts.db.engine.connect() as conn:
        existing = conn.execute(select(t).where(t.c.id == transfer_id)).mappings().first()
    if existing:
        if str(existing['account_id']) != str(account['id']):
            raise ServiceError('Request belongs to another account', 409)
        return public_record(existing)
    data = validate_request(payload, account)
    enrich_positions(data)
    data['submittedBy'] = str(getattr(g, 'current_principal_id', ''))
    with accounts.db.engine.begin() as conn:
        record = conn.execute(insert(t).values(id=transfer_id, account_id=account['id'], master_account=account['master_account'],
            ibkr_account_id=account['ibkr_account_number'], request_data=data, submission_state='SUBMITTING', created=now(), updated=now())
            .on_conflict_do_nothing(index_elements=['id']).returning(t)).mappings().first()
    if record is None:
        return public_record(read_record(transfer_id, account['id']))
    record = dict(record)
    try:
        result = accounts.ibkr_web_api.transfer_positions_externally_complex(
            account_id=record['ibkr_account_id'], master_account=record['master_account'],
            positions=data['positions'], contra_broker_info=data['contraBrokerInfo'], currency=data['currency'],
            trade_date=data['nonDisclosedDetail']['tradeDate'], settle_date=data['nonDisclosedDetail']['settleDate'],
            pset_bic=data['nonDisclosedDetail']['psetBic'], client_instruction_id=record['client_instruction_id'])
        fields = response_fields(result, record)
    except ServiceError as error:
        if error.status_code in (400, 403, 422):
            return save_fields(record, {'submission_state': 'REJECTED', 'last_error': {
                'code': error.code or 'ibkr_request_rejected', 'message': str(error)}})
        return save_fields(record, {'submission_state': 'UNKNOWN', 'last_error': {
            'code': 'submission_outcome_unknown', 'message': 'Submission outcome is uncertain. Refresh this saved instruction; do not create another transfer.'}})
    except Exception:
        # The durable original ID is the recovery key. Never send a new POST on uncertainty.
        return save_fields(record, {'submission_state': 'UNKNOWN', 'last_error': {
            'code': 'submission_outcome_unknown', 'message': 'Submission outcome is uncertain. Refresh this saved instruction; do not create another transfer.'}})
    return save_fields(record, fields)


def refresh_transfer(payload):
    if not isinstance(payload, dict):
        raise ServiceError('JSON object required', 400)
    record = read_record(payload.get('transfer_id'), payload.get('account_id'))
    response = accounts.ibkr_web_api.get_external_transfer_status(master_account=record['master_account'],
        instruction_id=record['instruction_id'], client_instruction_id=str(record['client_instruction_id']))
    return save_fields(record, response_fields(response, record))


def save_ticket(payload):
    if not isinstance(payload, dict):
        raise ServiceError('JSON object required', 400)
    record = read_record(payload.get('transfer_id'), payload.get('account_id'))
    if not record['instruction_id']:
        raise ServiceError('An official IBKR instruction ID is required before saving a ticket', 409)
    reference = text(payload.get('message_center_ticket_reference'), 'Message Center ticket reference')
    fields = {'message_center_ticket_reference': reference, 'ticket_recorded_at': now()}
    if payload.get('ticket_created_at'):
        try:
            fields['ticket_created_at'] = datetime.fromisoformat(payload['ticket_created_at']).isoformat()
        except (ValueError, TypeError):
            raise ServiceError('Ticket creation time must be an ISO timestamp', 400)
    for name in ('ticket_url', 'portal_transfer_reference'):
        if payload.get(name):
            fields[name] = text(payload[name], name, 512)
    if 'ticket_url' in fields and not fields['ticket_url'].startswith('https://'):
        raise ServiceError('Ticket link must use HTTPS', 400)
    if isinstance(payload.get('receiving_broker_bmo_acceptance'), bool):
        fields['receiving_broker_bmo_acceptance'] = payload['receiving_broker_bmo_acceptance']
    return save_fields(record, fields)
