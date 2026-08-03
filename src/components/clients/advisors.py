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
def update_advisor(query: dict = None, advisor: dict = None):
    """Update an advisor record selected by query."""
    if query is None:
        raise ServiceError('Query must be provided', status_code=400)
    if advisor is None:
        raise ServiceError('Advisor must be provided', status_code=400)
    unsupported_fields = set(advisor) - {'contact_id'}
    if unsupported_fields:
        raise ServiceError('Only contact_id can be updated through this route', status_code=400)

    advisor_id = db.update(table='advisor', query=query, data=advisor)
    return {'id': advisor_id}

@handle_exception
def read_advisors(query=None):
    advisors = db.read(table='advisor', query=query)
    return advisors
