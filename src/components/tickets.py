from src.utils.exception import handle_exception    
from src.utils.connectors.supabase import db
from src.utils.managers.document_center import DocumentCenter
from src.utils.logger import logger
from components.documents.client_documents import upload_poa, upload_poi

logger.announcement('Initializing Tickets Service', type='info')
documents = DocumentCenter('clients')
logger.announcement('Initialized Tickets Service', type='success')

@handle_exception
def create_ticket(ticket: dict = None, ticket_info: dict = None) -> dict:
    ticket_id = db.create(table='ticket', data=ticket)
    ticket_info['ticket_id'] = ticket_id
    ticket_info = db.create(table='individual_ticket', data=ticket_info)
    return {'ticket': ticket, 'info': ticket_info}

@handle_exception
def read_tickets(query=None) -> list:
    tickets = db.read(table='ticket', query=query)
    return tickets

@handle_exception
def read_ticket_info(query=None) -> dict:
    ticket_info = db.read(table='individual_ticket', query=query)
    if len(ticket_info) == 1:
        return ticket_info[0]
    else:
        raise Exception('Ticket Info has zero, two or more matches.')

@handle_exception
def read_ticket_docs(ticket_id: str = None, query: dict = None) -> list:
    """
    Reads the POA and POI documents for a given ticket
    """

    query = {'ticket_id': ticket_id, **query}

    ticket = db.read(table='individual_ticket', query=query)
    if len(ticket) != 1:
        raise Exception('Ticket has zero, two or more matches.')

    docs = documents.read(query)
    
    return docs

@handle_exception
def update_ticket_info(ticket_info: dict = None, query: dict = None) -> str:
    ticket_id = db.update(table='individual_ticket', data=ticket_info, query=query)
    return ticket_id

@handle_exception
def upload_ticket_poa(f: dict = None, document_info: dict = None, user_id: str = None, ticket_id: str = None, ticket_info: dict = None) -> str:
    poa_id = upload_poa(f=f, document_info=document_info, user_id=user_id)
    ticket_info['poa_id'] = poa_id
    return update_ticket_info(ticket_info=ticket_info, query={'ticket_id': ticket_id})

@handle_exception
def upload_ticket_poi(f: dict = None, document_info: dict = None, user_id: str = None, ticket_id: str = None, ticket_info: dict = None) -> str:
    poi_id = upload_poi(f=f, document_info=document_info, user_id=user_id)
    ticket_info['poi_id'] = poi_id
    return update_ticket_info(ticket_info=ticket_info, query={'ticket_id': ticket_id})