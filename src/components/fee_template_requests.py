from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger

logger.announcement('Initializing Fee Template Requests Service', type='info')
logger.announcement('Initialized Fee Template Requests Service', type='success')

@handle_exception
def create_fee_template_request(fee_template_request: dict = None):
    fee_template_request_id = db.create(table='fee_template_request', data=fee_template_request)
    return fee_template_request_id

@handle_exception
def read_fee_template_requests(query=None):
    fee_template_requests = db.read(table='fee_template_request', query=query)
    return fee_template_requests

@handle_exception
def update_fee_template_request(fee_template_request_id: str, data: dict):
    fee_template_request = db.update(table='fee_template_request', query={'id': fee_template_request_id}, data=data)
    return fee_template_request

@handle_exception
def delete_fee_template_request(fee_template_request_id: str):
    fee_template_request = db.delete(table='fee_template_request', query={'id': fee_template_request_id})
    return fee_template_request