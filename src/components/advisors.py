from src.utils.exception import handle_exception

from ..helpers.drive import GoogleDrive
from ..helpers.database import Firebase
from src.utils.logger import logger

logger.announcement('Initializing Advisors Service', type='info')
logger.announcement('Initialized Advisors Service', type='success')

Drive = GoogleDrive()
Database = Firebase()

@handle_exception
def read_commissions():
    return Drive.download_file('1PAVYRFTbTqsRQFiFYW0Ro82BgGTe2X6g', True)

@handle_exception
def read_advisors(query=None):
    return Database.read('db/advisors/dictionary', query)