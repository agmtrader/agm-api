"""Staff-reviewed IBKR task and SLS notices with a dedicated send history."""
from datetime import datetime, timezone
from hashlib import sha256
import json
import re
from uuid import UUID

from src.components.tools.public.reporting import get_clients_report, get_ibkr_details, get_tasks_review
from src.utils.connectors.gmail import GmailConnector
from src.utils.connectors.supabase import db
from src.utils.exception import ServiceError
from src.utils.task_review import state_group

TABLE = 'sls_outreach_email'
HOLDER_ROLES = {
    'accountholder', 'firstholder', 'secondholder', 'primaryholder', 'secondaryholder',
    'firstaccountholder', 'secondaccountholder', 'primaryaccountholder', 'secondaryaccountholder',
}


def identifier(value):
    try:
        return str(UUID(value))
    except (ValueError, AttributeError, TypeError):
        raise ServiceError('Invalid outreach identifier.', 400) from None


def text(value):
    return str(value or '').strip()


def account_details(account_id):
    accounts = db.read('account', {'id': account_id})
    if len(accounts) != 1:
        raise ServiceError('Account not found.', 404)
    return accounts[0]


def holder_people(account):
    number, master = text(account['ibkr_account_number']).upper(), text(account.get('master_account')).upper()
    if not number:
        return []
    matches = []
    for row in get_ibkr_details():
        if not isinstance(row, dict):
            continue
        details = row.get('ibkrdetails')
        for detail in details if isinstance(details, list) else [row]:
            if not isinstance(detail, dict):
                continue
            broker = detail.get('account') or {}
            if text(broker.get('accountId')).upper() == number and (
                not master or text(broker.get('masterAccountId')).upper() == master
            ):
                matches.append(detail)
    if len(matches) != 1:
        return []
    return [person for person in matches[0].get('associatedPersons') or [] if isinstance(person, dict)]


def contacts(account):
    links = db.read('account_contact', {'account_id': str(account['id'])})
    if not links:
        return {}
    people = holder_people(account)
    if not people:
        return {}
    contact_ids = list({str(link['contact_id']) for link in links})
    by_id = {str(contact['id']): contact for contact in db.read('contact', {'id': contact_ids})}
    recipients = {}
    for link in links:
        contact_id = str(link['contact_id'])
        contact = by_id.get(contact_id, {})
        email = text(contact.get('email'))
        if not re.fullmatch(r'[^\s@,;<>]+@[^\s@,;<>]+\.[^\s@,;<>]+', email):
            continue
        entity, external = text(link.get('entity_id')), text(link.get('external_id'))
        if entity or external:
            matches = [person for person in people if (
                (entity and entity == text(person.get('entityId'))) or
                (external and external == text(person.get('externalCode')))
            )]
        else:
            matches = [person for person in people if email.casefold() == text(person.get('email')).casefold()]
        if len(matches) != 1:
            continue
        person = matches[0]
        roles = {re.sub(r'[^a-z]', '', text(role).lower()) for role in (person.get('associations') or [])}
        if not roles.intersection(HOLDER_ROLES) or 'trustedcontact' in roles:
            continue
        person_ids = {text(person.get('entityId')), text(person.get('externalCode'))} - {''}
        if contact_id in recipients and recipients[contact_id]['entities'] != person_ids:
            # A single CRM contact linked to multiple people cannot be addressed safely.
            recipients[contact_id]['ambiguous'] = True
        else:
            recipients.setdefault(contact_id, {'id': contact_id, 'name': contact['name'], 'email': email, 'entities': person_ids})
    return {key: recipient for key, recipient in recipients.items() if not recipient.get('ambiguous')}


def outreach_context(account_id, offset):
    account = account_details(account_id)
    recipient_error = None
    try:
        recipients = contacts(account)
    except ServiceError:
        recipients = {}
        recipient_error = 'Holder verification is unavailable. Reload before preparing an email.'
    return {
        'contacts': [{key: recipient[key] for key in ('id', 'name', 'email')}
                     for recipient in recipients.values()],
        'recipient_error': recipient_error,
        'history': db.read(TABLE, {'account_id': account_id}, offset=offset, limit=50,
                           exclude_columns=['fingerprint', 'message', 'tasks'],
                           order_by=('created', 'id')),
    }


def pending_observations(number):
    report = get_tasks_review({'account_id': number, 'status': 'pending', 'limit': '200'})
    if report['total'] > len(report['rows']):
        raise ServiceError('Too many task observations to review safely for this account.', 409)
    return report['rows']


def eligible_tasks(observations, entities):
    eligible = []
    seen = set()
    for row in sorted(observations, key=lambda item: item['source'] == 'compliance'):
        state = state_group(row['state'])
        if row['completed'] or row['freshness'] != 'fresh' or state != 'pending':
            continue
        if row.get('person') and str(row['person']).strip() not in entities:
            continue
        if row['warning'] and not row['related_ids']:
            continue
        if row['warning'].startswith('Source states differ'):
            continue
        if row['source'] == 'compliance' and row['related_ids']:
            # Prefer the linked broker observation, including its holder match.
            continue
        identity = (row['source'], row['task_number'] or row['id'])
        if identity in seen or not row['title'].strip() or row['title'] == 'Unnamed task':
            continue
        seen.add(identity)
        eligible.append({key: row[key] for key in ('id', 'source', 'title', 'task_number', 'form_number', 'last_success_at')})
    return eligible


def verify_sls_candidate(number):
    rows = [row for row in get_clients_report()
            if str(row.get('Account ID') or '').strip().upper() == number]
    if len(rows) != 1 or str(rows[0].get('Status') or '').strip().lower() != 'open':
        raise ServiceError('SLS outreach requires a uniquely matched open account.', 409)
    value = rows[0].get('SLS Devices')
    if not isinstance(value, str) or value.strip() not in ('', '-'):
        raise ServiceError('The Clients report no longer identifies this account as missing a recorded device.', 409)


def preview(account_id, payload):
    if not isinstance(payload, dict):
        raise ServiceError('An outreach request is required.', 400)
    purpose, language = payload.get('purpose'), payload.get('language')
    if purpose not in ('tasks', 'sls', 'both') or language not in ('es', 'en'):
        raise ServiceError('Select a valid notice type and language.', 400)
    account = account_details(account_id)
    recipient = contacts(account).get(identifier(payload.get('contact_id')))
    if recipient is None:
        raise ServiceError('Select a linked account holder with a valid contact email.', 400)
    number = text(account.get('ibkr_account_number')).upper()
    if not number:
        raise ServiceError('The account has no saved IBKR account number.', 409)
    tasks = []
    if purpose in ('tasks', 'both'):
        tasks = eligible_tasks(pending_observations(number), recipient['entities'])
        if not tasks:
            raise ServiceError('No fresh, actionable pending tasks were found for this holder.', 409)
    if purpose in ('sls', 'both'):
        verify_sls_candidate(number)

    spanish = language == 'es'
    subject = 'Acciones pendientes en su cuenta IBKR' if spanish else 'Actions pending in your IBKR account'
    lines = [f"{'Hola' if spanish else 'Hello'} {recipient['name']},"]
    if tasks:
        lines.append(f"{'Revise estas tareas pendientes en Client Portal para la cuenta' if spanish else 'Review these pending tasks in Client Portal for account'} {number}:")
        lines.extend(f"• {task['title']}" + (f" ({task['form_number']})" if task['form_number'] else '') for task in tasks)
    if purpose in ('sls', 'both'):
        lines.append(
            f"Para la cuenta {number}, active la autenticación de dos factores con IBKR Mobile mediante la opción IB Key."
            if spanish else
            f"For account {number}, please activate two-factor authentication in IBKR Mobile using IB Key."
        )
    lines.append('Si necesita ayuda, responda a este correo.\nAGM Technology' if spanish else 'If you need help, reply to this email.\nAGM Technology')
    notice = {
        'account_id': account_id, 'contact_id': recipient['id'], 'recipient_email': recipient['email'],
        'purpose': purpose, 'language': language, 'subject': subject, 'message': '\n\n'.join(lines), 'tasks': tasks,
    }
    snapshot = {**notice, 'tasks': [{key: value for key, value in task.items() if key != 'last_success_at'} for task in tasks]}
    fingerprint = sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()
    return {**notice, 'fingerprint': fingerprint}


def send(account_id, actor_id, payload):
    if not isinstance(payload, dict):
        raise ServiceError('An outreach request is required.', 400)
    attempt_id = identifier(payload.get('request_id'))
    existing = db.read(TABLE, {'id': attempt_id, 'account_id': account_id, 'actor_id': actor_id})
    if existing:
        return existing[0]
    if payload.get('recipient_confirmed') is not True or (
        payload.get('purpose') in ('sls', 'both') and payload.get('sls_confirmed') is not True
    ):
        raise ServiceError('Confirm the recipient and, for an SLS notice, the current device status in IBKR.', 400)

    notice = preview(account_id, payload)
    if payload.get('fingerprint') != notice['fingerprint']:
        raise ServiceError('The recipient or outstanding actions changed. Generate a new preview.', 409)
    unresolved = db.read(TABLE, {'fingerprint': notice['fingerprint'], 'status': ['pending', 'unknown']})
    if unresolved:
        raise ServiceError('An earlier send for this notice is unresolved. Check Gmail before sending again.', 409)
    gmail = GmailConnector()
    raw = gmail.create_html_email(notice['message'], notice['subject'], notice['recipient_email'], 'sls_outreach')
    record = {**notice, 'id': attempt_id, 'actor_id': actor_id, 'status': 'pending'}
    # The primary key claims a request before contacting Gmail. Concurrent or uncertain
    # repeats with this ID cannot send again, even if the recipient never saw a response.
    try:
        db.create(TABLE, record)
    except ServiceError as exc:
        if (exc.details or {}).get('exception_type') != 'IntegrityError':
            raise
        existing = db.read(TABLE, {'id': attempt_id, 'account_id': account_id, 'actor_id': actor_id})
        if existing:
            return existing[0]
        raise ServiceError('Another send may be in progress. Reload email history before retrying.', 409) from None
    try:
        receipt = gmail.service.users().messages().send(userId='me', body=raw).execute(num_retries=0)
        message_id = receipt['id']
        if not message_id:
            raise ValueError('Missing provider receipt')
    except Exception:
        db.update(TABLE, {'id': attempt_id}, {'status': 'unknown'})
        raise ServiceError('Gmail acceptance could not be confirmed. Check history and Gmail before sending again.', 502) from None
    result = {
        'status': 'sent', 'provider_message_id': message_id,
        'sent_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
    }
    db.update(TABLE, {'id': attempt_id}, dict(result))
    return {**record, **result}
