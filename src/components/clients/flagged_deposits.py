from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger

logger.announcement('Initializing Flagged Deposits Service', type='info')
logger.announcement('Initialized Flagged Deposits Service', type='success')

@handle_exception
def create_flagged_deposit(flagged_deposit: dict = None):
    flagged_deposit_id = db.create(table='flagged_deposit', data=flagged_deposit)
    return {'id': flagged_deposit_id}

@handle_exception
def read_flagged_deposits(query=None):
    flagged_deposits = db.read(table='flagged_deposit', query=query)
    return flagged_deposits

@handle_exception
def update_flagged_deposit(query=None, flagged_deposit=None):
    flagged_deposit_id = db.update(table='flagged_deposit', query=query, data=flagged_deposit)
    return {'id': flagged_deposit_id}