from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger

logger.announcement('Initializing Contacts Service', type='info')
logger.announcement('Initialized Contacts Service', type='success')

@handle_exception
def create_contact(contact: dict = None):
    contact_id = db.create(table='contact', data=contact)
    return {'id': contact_id}

@handle_exception
def read_contacts(query=None):
    contacts = db.read(table='contact', query=query)
    return contacts