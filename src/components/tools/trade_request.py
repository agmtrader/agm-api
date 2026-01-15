from src.utils.connectors.supabase import db
from src.utils.logger import logger
from src.utils.exception import handle_exception

logger.announcement('Initializing Trade Requests Service', type='info')
logger.announcement('Initialized Trade Requests Service', type='success')

@handle_exception
def create_trade_request(side: str, quantity: int, order_type: str, time_in_force: str):
    logger.info('Creating trade request. Processing data...')
    trade_request = db.create(table='trade_request', data={'side': side, 'quantity': quantity, 'order_type': order_type, 'time_in_force': time_in_force})
    return trade_request

@handle_exception
def read_trade_request(id: str):
    query = {}
    if id:
        query['id'] = id
    trade_request = db.read(table='trade_request', query=query)
    return trade_request