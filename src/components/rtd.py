from src.utils.exception import handle_exception
from src.utils.connectors.drive import GoogleDrive
from src.utils.logger import logger

logger.announcement('Initializing RTD Service', type='info')
drive = GoogleDrive()
logger.announcement('Initialized RTD Service', type='success')

@handle_exception
def read_rtd():
    rtd = drive.get_file_info_by_id("12q0kTiRN3j-20iCxnPXAzyXmHd5kE-jv")
    return rtd