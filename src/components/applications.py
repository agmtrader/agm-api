from src.utils.exception import handle_exception
from src.utils.connectors.firebase import Firebase
from src.utils.connectors.ibkr_web_api import IBKRWebAPI
from src.utils.logger import logger

logger.announcement('Initializing Applications Service', type='info')
db = Firebase()
ibkr_web_api = IBKRWebAPI()
logger.announcement('Initialized Applications Service', type='success')

@handle_exception
def create_application(application: dict = None) -> dict:
    application_id = db.create(data=application, path='applications')
    return {'id': application_id}

@handle_exception
def read_applications(query=None) -> list:
    applications = db.read(path='applications', query=query)
    return applications

@handle_exception
def update_application(application: dict = None) -> dict:
    return db.update(path='applications', data=application)

@handle_exception
def send_to_ibkr(application: dict = None) -> dict:
    return ibkr_web_api.send_to_ibkr(application=application)