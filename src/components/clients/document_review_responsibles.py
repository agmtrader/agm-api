from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db

TABLE = 'document_review_responsible'


@handle_exception
def create_document_review_responsible(document_review_responsible: dict = None):
    if not document_review_responsible:
        raise Exception('document_review_responsible payload is required')

    required_keys = ['account_id', 'contact_id', 'user_id']
    missing_keys = [key for key in required_keys if not document_review_responsible.get(key)]
    if missing_keys:
        raise Exception(f"Missing required fields: {', '.join(missing_keys)}")

    existing = db.read(
        table=TABLE,
        query={
            'account_id': document_review_responsible['account_id'],
            'contact_id': document_review_responsible['contact_id'],
            'user_id': document_review_responsible['user_id'],
        },
    )

    if existing:
        return {'id': existing[0]['id'], 'status': 'exists'}

    record_id = db.create(table=TABLE, data=document_review_responsible)
    return {'id': record_id, 'status': 'created'}


@handle_exception
def upsert_document_review_responsible(account_id: str = None, contact_id: str = None, user_id: str = None, comment: str = None):
    if not account_id:
        raise Exception('account_id is required')
    if not contact_id:
        raise Exception('contact_id is required')

    existing_rows = db.read(
        table=TABLE,
        query={
            'account_id': account_id,
            'contact_id': contact_id,
        },
    )

    normalized_comment = comment if isinstance(comment, str) and comment.strip() else None
    normalized_user_id = user_id if user_id else None

    if existing_rows:
        existing_row = existing_rows[0]
        db.update(
            table=TABLE,
            query={'id': existing_row['id']},
            data={
                'user_id': normalized_user_id,
                'comment': normalized_comment,
            },
        )
        return {'id': existing_row['id'], 'status': 'updated'}

    created_id = db.create(
        table=TABLE,
        data={
            'account_id': account_id,
            'contact_id': contact_id,
            'user_id': normalized_user_id,
            'comment': normalized_comment,
        },
    )
    return {'id': created_id, 'status': 'created'}


@handle_exception
def read_document_review_responsibles(query=None):
    return db.read(table=TABLE, query=query or {})


@handle_exception
def delete_document_review_responsible(query: dict = None):
    if not query:
        raise Exception('query is required')

    deleted_id = db.delete(table=TABLE, query=query)
    return {'id': deleted_id, 'status': 'deleted'}
