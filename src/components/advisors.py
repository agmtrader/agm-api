from src.utils.exception import handle_exception

from .drive import GoogleDrive
from src.utils.logger import logger

Drive = GoogleDrive()

@handle_exception
def get_commissions():
    response = Drive.download_file('1PAVYRFTbTqsRQFiFYW0Ro82BgGTe2X6g', True)
    return response