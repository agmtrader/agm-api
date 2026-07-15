from src.utils.exception import ServiceError, handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger

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
