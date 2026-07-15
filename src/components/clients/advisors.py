from src.utils.exception import ServiceError, handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger
from datetime import datetime
from uuid import UUID
import re

logger.announcement('Initializing Advisors Service', type='info')
logger.announcement('Initialized Advisors Service', type='success')

@handle_exception
def create_advisor(advisor: dict = None):
    advisor_id = db.create(table='advisor', data=advisor)
    return {'id': advisor_id}

@handle_exception
def read_advisors(query=None):
    advisors = db.read(table='advisor', query=query)
    return advisors

@handle_exception
def update_advisor(query=None, advisor=None):
    advisor_id = db.update(table='advisor', query=query, data=advisor)
    return {'id': advisor_id}


@handle_exception
def link_advisor_contact(advisor_id: str = None, contact_id: str = None):
    """Link an existing advisor to an existing contact and return the saved advisor."""
    if not advisor_id or not contact_id:
        raise ServiceError('advisor_id and contact_id are required', status_code=400)

    advisors = db.read(table='advisor', query={'id': advisor_id})
    if not advisors:
        raise ServiceError('Advisor not found', status_code=404)

    contacts = db.read(table='contact', query={'id': contact_id})
    if not contacts:
        raise ServiceError('Contact not found', status_code=404)

    db.update(
        table='advisor',
        query={'id': advisor_id},
        data={'contact_id': contact_id},
    )

    updated_advisors = db.read(table='advisor', query={'id': advisor_id})
    if not updated_advisors or updated_advisors[0].get('contact_id') != contact_id:
        raise ServiceError('Advisor contact link could not be verified', status_code=500)

    return updated_advisors[0]


def _serialize_record(record) -> dict:
    result = {
        column.name: getattr(record, column.name)
        for column in record.__table__.columns
    }
    for key, value in result.items():
        if (key == 'id' or key.endswith('_id')) and value is not None:
            result[key] = str(value)
    return result


@handle_exception
def create_and_link_advisor_contact(advisor_id: str = None, contact: dict = None):
    """Create a contact and link it to an advisor in one database transaction."""
    if not advisor_id:
        raise ServiceError('advisor_id is required', status_code=400)

    try:
        advisor_uuid = UUID(str(advisor_id))
    except (TypeError, ValueError):
        raise ServiceError('advisor_id must be a valid UUID', status_code=400)

    contact = contact or {}
    name = str(contact.get('name') or '').strip()
    email = str(contact.get('email') or '').strip()
    phone = str(contact.get('phone') or '').strip() or None

    if not name:
        raise ServiceError('Contact name is required', status_code=400)
    if not re.fullmatch(r'[^@\s]+@[^@\s]+\.[^@\s]+', email):
        raise ServiceError('A valid contact email is required', status_code=400)

    if not db.read(table='advisor', query={'id': str(advisor_uuid)}):
        raise ServiceError('Advisor not found', status_code=404)
    if db.read(table='contact', query={'email': email}):
        raise ServiceError('A contact with this email already exists', status_code=400)

    advisor_model = next(
        (model for model in db.base.__subclasses__() if getattr(model, '__tablename__', None) == 'advisor'),
        None,
    )
    contact_model = next(
        (model for model in db.base.__subclasses__() if getattr(model, '__tablename__', None) == 'contact'),
        None,
    )
    if advisor_model is None or contact_model is None:
        raise ServiceError('Advisor contact models are unavailable', status_code=500)

    @db.with_session
    def _create_and_link(session):
        advisor_record = (
            session.query(advisor_model)
            .filter(advisor_model.id == advisor_uuid)
            .with_for_update()
            .one_or_none()
        )
        if advisor_record is None:
            raise RuntimeError('Advisor disappeared before the contact could be linked')

        current_time = datetime.now().strftime('%Y%m%d%H%M%S')
        contact_record = contact_model(
            created=current_time,
            updated=current_time,
            name=name,
            email=email,
            phone=phone,
        )
        session.add(contact_record)
        session.flush()

        advisor_record.contact_id = contact_record.id
        advisor_record.updated = current_time
        session.flush()

        return {
            'advisor': _serialize_record(advisor_record),
            'contact': _serialize_record(contact_record),
        }

    return _create_and_link()
