from src.utils.exception import handle_exception

from .drive import GoogleDrive
from .database import Firebase

Drive = GoogleDrive()
Database = Firebase()

@handle_exception
def read_commissions():
    response = Drive.download_file('1PAVYRFTbTqsRQFiFYW0Ro82BgGTe2X6g', True)
    return response

@handle_exception
def read_advisors(query=None):
    response = Database.read('db/advisors/dictionary', query)
    return response