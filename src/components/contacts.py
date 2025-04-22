from src.utils.exception import handle_exception
from src.helpers.database import Firebase
from src.utils.logger import logger
Database = Firebase()

logger.announcement('Initializing Contacts Service', type='info')
logger.announcement('Initialized Contacts Service', type='success')

class Contact:
    def __init__(self, data):
        self.contact_id = data['ContactID']
        self.name = data['ContactName']
        self.email = data.get('ContactEmail', '')
        self.phone = data.get('ContactPhone', '')
        self.country = data.get('ContactCountry', '')
    
@handle_exception
def create_contact(data, id):
    contact = Contact(data)
    contact = Database.create(path='db/clients/contacts', data=data, id=id)
    return contact

@handle_exception
def read_contacts(query=None):
    contacts = Database.read(path='db/clients/contacts', query=query)
    return contacts

@handle_exception
def update_contact(data, query=None):
    contact = Database.update(path='db/clients/contacts', data=data, query=query)
    return contact

@handle_exception
def delete_contact(id):
    contact = Database.delete(path='db/clients/contacts', query={'ContactID': id})
    return contact