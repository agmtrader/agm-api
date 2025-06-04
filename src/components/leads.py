from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger

logger.announcement('Initializing Leads Service', type='info')
logger.announcement('Initialized Leads Service', type='success')
    
@handle_exception
def create_lead(lead: dict = None, follow_ups: list = None):
    
    lead_id = db.create(table='lead', data=lead)
    
    for follow_up in follow_ups:
        follow_up['lead_id'] = lead_id
        db.create(table='follow_up', data=follow_up)
    
    return {'id': lead_id}

@handle_exception
def read_leads(query: dict = None):
    leads = db.read(table='lead', query=query)
    follow_ups = db.read(table='follow_up', query=query)

    filtered_follow_ups = []
    for lead in leads:
        for follow_up in follow_ups:
            if follow_up['lead_id'] == lead['id']:
                filtered_follow_ups.append(follow_up)

    return {'leads': leads, 'follow_ups': filtered_follow_ups}

@handle_exception
def read_follow_ups(query: dict = None):
    follow_ups = db.read(table='follow_up', query=query)
    return follow_ups

@handle_exception
def update_lead(query: dict = None, lead: dict = None):
    lead_id = db.update(table='lead', query=query, data=lead)
    return {'id': lead_id}

@handle_exception
def update_follow_up(lead_id: str = None, follow_up: dict = None):
    follow_up_id = db.update(table='follow_up', query={'lead_id': lead_id}, data=follow_up)
    return {'id': follow_up_id}

@handle_exception
def delete_lead(query: dict = None):
    lead_id = db.delete(table='lead', query=query)
    return {'id': lead_id}