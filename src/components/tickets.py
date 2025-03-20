from src.utils.exception import handle_exception
import json
from src.helpers.database import Firebase
from src.utils.logger import logger
Database = Firebase()

logger.announcement('Initializing Tickets Service', type='info')
logger.announcement('Initialized Tickets Service', type='success')

@handle_exception
def create_ticket(data, id):
    ticket = Database.create(path='db/clients/tickets', data=data, id=id)
    ticket = json.loads(ticket.data.decode('utf-8'))
    return ticket

@handle_exception
def read_tickets(query=None):
    tickets = Database.read(path='db/clients/tickets', query=query)
    tickets = json.loads(tickets.data.decode('utf-8'))
    return tickets

@handle_exception
def update_ticket(data, query=None):
    ticket = Database.update(path='db/clients/tickets', data=data, query=query)
    ticket = json.loads(ticket.data.decode('utf-8'))
    return ticket