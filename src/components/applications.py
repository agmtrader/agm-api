from src.utils.exception import handle_exception
from src.utils.connectors.ibkr_web_api import IBKRWebAPI
from src.utils.connectors.supabase import db
from src.utils.connectors.drive import GoogleDrive
from src.utils.logger import logger

logger.announcement('Initializing Applications Service', type='info')
ibkr_web_api = IBKRWebAPI()
google_drive = GoogleDrive()
logger.announcement('Initialized Applications Service', type='success')

@handle_exception
def create_application(application: dict = None) -> dict:
    application_id = db.create(table='application', data=application)
    return {'id': application_id}

@handle_exception
def read_applications(query=None) -> list:
    applications = db.read(table='application', query=query)
    return applications

@handle_exception
def update_application(application: dict = None, query: dict = None) -> dict:
    return db.update(table='application', query=query, data=application)


# IBKR Web API
@handle_exception
def send_to_ibkr(application: dict = None, master_account: str = None) -> dict:
    return ibkr_web_api.send_to_ibkr(application={'application': application}, master_account=master_account)