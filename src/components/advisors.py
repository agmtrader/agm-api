from src.utils.exception import handle_exception

from ..helpers.database import Firebase
from src.utils.logger import logger

logger.announcement('Initializing Advisors Service', type='info')
logger.announcement('Initialized Advisors Service', type='success')

Database = Firebase()

@handle_exception
def read_advisors(query=None):
    return Database.read('db/advisors/dictionary', query)