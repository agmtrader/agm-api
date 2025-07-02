from src.utils.logger import logger
from src.utils.connectors.supabase import db
from src.utils.exception import handle_exception

logger.announcement('Initializing Risk Profile Service', type='info')
logger.announcement('Initialized Risk Profile Service', type='success')

@handle_exception
def create_risk_profile(data: dict):
    risk_profile_id = db.create(table='account_risk_profile', data=data)
    return {'id': risk_profile_id}

@handle_exception
def read_risk_profiles(query: dict = None):
    risk_profiles = db.read(table='account_risk_profile', query=query)
    return risk_profiles