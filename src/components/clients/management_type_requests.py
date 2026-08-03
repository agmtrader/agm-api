from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger

logger.announcement('Initializing Management Type Requests Service', type='info')
logger.announcement('Initialized Management Type Requests Service', type='success')


@handle_exception
def create_management_type_request(management_type_request: dict = None):
    """Create a new management_type_request record.

    Parameters
    ----------
    management_type_request : dict
        Dictionary with management type request fields.

    Returns
    -------
    dict
        Created record id.
    """
    management_type_request_id = db.create(table='management_type_request', data=management_type_request)
    return {'id': management_type_request_id}

