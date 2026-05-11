from src.utils.exception import handle_exception
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