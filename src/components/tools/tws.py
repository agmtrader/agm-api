from src.utils.exception import handle_exception
from src.utils.connectors.tws import TWS
from src.utils.logger import logger

tws = TWS()

logger.announcement('Initializing Trader Workstation Service', type='info')
logger.announcement('Initialized Trader Workstation Service', type='success')

@handle_exception
def get_account_summary():
    return tws.get_account_summary()