from src.utils.logger import logger
from src.helpers.database import Firebase
from src.utils.exception import handle_exception
import json

logger.announcement('Initializing Risk Profile Service', type='info')
logger.announcement('Initialized Risk Profile Service', type='success')

Database = Firebase()

@handle_exception
def create_risk_profile(data: dict, id: str):
    Database.create(path='db/clients/risk_profiles', data=data, id=id)
    return

@handle_exception
def read_risk_profiles():
    risk_profiles = Database.read(path='db/clients/risk_profiles')
    return risk_profiles