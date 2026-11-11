"""Manual review history for transactions whose source remains the Drive report."""
import json
import base64
from email.mime.text import MIMEText
from datetime import date
from uuid import UUID, uuid4

from sqlalchemy import text
from src.utils.connectors.supabase import db
from src.utils.exception import ServiceError
from src.utils.connectors.gmail import GmailConnector
from src.utils.logger import logger

STATUSES = {'flagged', 'escalated', 'resolved'}
SNAPSHOT_FIELDS = ('date', 'amount', 'currency', 'direction', 'account', 'signals')


def notify_cris(transaction_id):
    """One Gmail attempt; a timeout is ambiguous and must not be retried automatically."""
    message = MIMEText('Please review the escalated transaction ' + transaction_id +
                       ' in the Deposits & Withdrawals report in the AGM Dashboard.', 'plain', 'utf-8')
    message['To'] = 'cr@agmtechnology.com'
    message['From'] = 'info@agmtechnology.com'
    message['Subject'] = 'Transaction review follow-up'
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode('ascii')
    response = GmailConnector().service.users().messages().send(userId='me', body={'raw': raw}).execute()
    if not response or not response.get('id'):
        raise RuntimeError('Gmail did not return a message identifier')


def list_events():
    with db.engine.connect() as conn:
        rows = conn.execute(text('SELECT * FROM agm_compliance.transaction_review_event ORDER BY created_at, id')).mappings()
        return [{**dict(row), 'id': str(row['id']), 'account_id': str(row['account_id']),
                 'parent_event_id': str(row['parent_event_id']) if row['parent_event_id'] else None,
                 'actor_id': str(row['actor_id'])} for row in rows]


def append_event(payload, user):
    if not isinstance(payload, dict):
        raise ServiceError('Invalid review request', 400)
    try:
        account_id = str(UUID(str(payload.get('account_id'))))
        actor_id = str(UUID(str(user.get('id'))))
    except (TypeError, ValueError):
        raise ServiceError('A valid account and reviewer are required', 400)
    transaction_id = str(payload.get('transaction_id') or '').strip()
    status = payload.get('status')
    note = payload.get('note')
    if not transaction_id or len(transaction_id) > 200 or status not in STATUSES:
        raise ServiceError('A transaction ID and valid status are required', 400)
    if not isinstance(note, str) or not note.strip() or len(note) > 4000:
        raise ServiceError('A note of up to 4000 characters is required', 400)
    follow_up_date = payload.get('follow_up_date') or None
    if follow_up_date:
        try:
            follow_up_date = date.fromisoformat(follow_up_date).isoformat()
        except (TypeError, ValueError):
            raise ServiceError('Invalid follow-up date', 400)
    if status == 'resolved':
        follow_up_date = None
    email = str(user.get('email') or '').strip()
    if not email:
        raise ServiceError('Reviewer email is unavailable', 403)
    snapshot = payload.get('transaction_snapshot')
    if not isinstance(snapshot, dict):
        raise ServiceError('Transaction details are required', 400)
    snapshot = {key: str(snapshot.get(key) or '')[:500] for key in SNAPSHOT_FIELDS}
    with db.engine.begin() as conn:
        # Serialize updates to the same account and transaction across workers.
        conn.execute(text('SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))'),
                     {'key': account_id + ':' + transaction_id})
        previous = conn.execute(text('SELECT status, transaction_snapshot FROM agm_compliance.transaction_review_event '
                                     'WHERE account_id = :account_id AND transaction_id = :transaction_id '
                                     "AND event_type = 'review' "
                                     'ORDER BY created_at DESC, id DESC LIMIT 1'),
                                {'account_id': account_id, 'transaction_id': transaction_id}).mappings().first()
        if previous is None and status != 'flagged':
            raise ServiceError('Flag the transaction before updating it', 409)
        if previous is not None:
            snapshot = previous['transaction_snapshot']
        row = conn.execute(text('INSERT INTO agm_compliance.transaction_review_event '
            '(id, account_id, transaction_id, status, event_type, note, follow_up_date, actor_id, actor_email, created_at, transaction_snapshot) '
            "VALUES (:id, :account_id, :transaction_id, :status, 'review', :note, :follow_up_date, :actor_id, :actor_email, to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'), CAST(:snapshot AS jsonb)) "
            'RETURNING id'), {'id': str(uuid4()), 'account_id': account_id, 'transaction_id': transaction_id,
            'status': status, 'note': note.strip(), 'follow_up_date': follow_up_date, 'actor_id': actor_id, 'actor_email': email,
            'snapshot': json.dumps(snapshot)}).first()
    result = {'id': str(row.id)}
    if status == 'escalated' and (previous is None or previous['status'] != 'escalated'):
        try:
            notify_cris(transaction_id)
            delivery = 'accepted'
        except Exception:
            # A provider timeout may occur after acceptance. Never silently retry.
            logger.warning('Transaction escalation email outcome could not be confirmed')
            delivery = 'unknown'
        try:
            with db.engine.begin() as conn:
                conn.execute(text('INSERT INTO agm_compliance.transaction_review_event '
                    '(id, account_id, transaction_id, status, event_type, parent_event_id, notification_status, note, actor_id, actor_email, created_at, transaction_snapshot) '
                    "VALUES (:id, :account_id, :transaction_id, 'escalated', 'notification', :parent_event_id, :delivery, :note, :actor_id, :actor_email, to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'), CAST(:snapshot AS jsonb))"),
                    {'id': str(uuid4()), 'account_id': account_id, 'transaction_id': transaction_id,
                     'parent_event_id': str(row.id),
                     'delivery': delivery, 'note': 'Escalation email to Cris: ' + delivery,
                     'actor_id': actor_id, 'actor_email': email,
                     'snapshot': json.dumps(snapshot)})
        except Exception:
            logger.warning('Transaction escalation email result could not be recorded')
            delivery = 'unknown'
        result['notification_status'] = delivery
    return result
