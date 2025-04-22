from src.utils.exception import handle_exception
import json
from src.helpers.database import Firebase
from src.utils.logger import logger
Database = Firebase()

logger.announcement('Initializing Leads Service', type='info')
logger.announcement('Initialized Leads Service', type='success')

class Lead:
    def __init__(self, data):
        self.lead_id = data['LeadID']
        self.contact_date = data['ContactDate']
        self.contact_id = data['ContactID']
        self.referrer = data['ReferrerID']
        self.description = data['Description']
        self.followups = data['FollowUps']
        self.completed = data['Completed']

    def to_dict(self):
        return {
            'LeadID': self.lead_id,
            'ContactDate': self.contact_date,
            'ContactID': self.contact_id,
            'ReferrerID': self.referrer,
            'Description': self.description,
            'FollowUps': self.followups,
            'Completed': self.completed
        }
        
@handle_exception
def create_lead(data, id):
    lead = Lead(data)
    lead = Database.create(path='db/clients/leads', data=data, id=id)
    return lead

@handle_exception
def read_leads(query=None):
    leads = Database.read(path='db/clients/leads', query=query)
    return leads

@handle_exception
def update_lead(data, query=None):
    lead = Database.update(path='db/clients/leads', data=data, query=query)
    return lead

@handle_exception
def delete_lead(query=None):
    lead = Database.delete(path='db/clients/leads', query=query)
    return lead