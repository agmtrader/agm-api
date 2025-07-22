from src.utils.exception import handle_exception
from src.utils.connectors.ibkr_web_api import IBKRWebAPI
from src.utils.connectors.mongodb import MongoDB
from src.utils.connectors.drive import GoogleDrive
from src.utils.logger import logger

logger.announcement('Initializing Applications Service', type='info')
db = MongoDB()
ibkr_web_api = IBKRWebAPI()
google_drive = GoogleDrive()
logger.announcement('Initialized Applications Service', type='success')

@handle_exception
def create_application(application: dict = None) -> dict:
    application_id = db.create(data=application, collection_name='application')
    return {'id': application_id}

@handle_exception
def read_applications(query=None) -> list:
    applications = db.read(collection_name='application', query=query)
    return applications

@handle_exception
def update_application(application: dict = None, query: dict = None) -> dict:
    return db.update(collection_name='application', update_data=application, query=query)

@handle_exception
def send_to_ibkr(application: dict = None) -> dict:
    logger.info(f"{application['customer']}")
    logger.info(f"{application['accounts']}")
    logger.info(f"{application['users']}")
    for document in application['documents']:
        logger.info(f"{document['attachedFile']['fileName']}")
    return ibkr_web_api.send_to_ibkr(application={'application':application})