from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger

logger.announcement('Initializing Advisor Changes Service', type='info')
logger.announcement('Initialized Advisor Changes Service', type='success')

@handle_exception
def create_advisor_change(advisor_change: dict = None):
    """Create a new advisor_change record.

    Parameters
    ----------
    advisor_change : dict
        Dictionary with advisor change fields.

    Returns
    -------
    dict
        Created record id.
    """
    advisor_change_id = db.create(table='advisor_change_request', data=advisor_change)
    return {'id': advisor_change_id}

@handle_exception
def read_advisor_changes(query=None):
    """Read advisor_change records matching query.

    Parameters
    ----------
    query : dict
        Query parameters.

    Returns
    -------
    list
        Advisor change records.
    """
    advisor_changes = db.read(table='advisor_change_request', query=query)
    return advisor_changes
