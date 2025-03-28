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
        self.name = data['Name']
        self.phone = data['Phone']
        self.email = data['Email']
        self.description = data['Description']
        self.followup_date = data['FollowupDate']
        self.completed = data['Completed']

    def to_dict(self):
        return {
            'LeadID': self.lead_id,
            'ContactDate': self.contact_date,
            'Name': self.name,
            'Phone': self.phone,
            'Email': self.email,
            'Description': self.description,
            'FollowupDate': self.followup_date,
            'Completed': self.completed
        }
        
@handle_exception
def create_lead(data, id):
    lead = Lead(data)
    lead = Database.create(path='db/leads/uploaded', data=data, id=id)
    return lead

@handle_exception
def read_leads(query=None):
    leads = Database.read(path='db/leads/uploaded', query=query)
    return leads

@handle_exception
def update_lead(data, query=None):
    lead = Database.update(path='db/leads/uploaded', data=data, query=query)
    return lead