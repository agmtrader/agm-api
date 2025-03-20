from src.utils.logger import logger
from src.helpers.database import Firebase
from src.utils.exception import handle_exception
import json

logger.announcement('Initializing Risk Profile Service', type='info')
logger.announcement('Initialized Risk Profile Service', type='success')

database = Firebase()

@handle_exception
def create_risk_profile(data: dict, id: str):
    database.create(path='db/clients/risk_profiles', data=data, id=id)
    return

@handle_exception
def read_risk_profiles():
    risk_profiles = database.read(path='db/clients/risk_profiles')
    risk_profiles = json.loads(risk_profiles.data.decode('utf-8'))
    return risk_profiles