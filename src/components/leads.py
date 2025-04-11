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
        print(data)
        self.name = data['Name']
        self.referrer = data['Referrer']
        self.phone_country = data['PhoneCountry']
        self.phone = data['Phone']
        self.email = data['Email']
        self.description = data['Description']
        self.followups = data['FollowUps']
        self.completed = data['Completed']

    def to_dict(self):
        return {
            'LeadID': self.lead_id,
            'ContactDate': self.contact_date,
            'Name': self.name,
            'Referrer': self.referrer,
            'PhoneCountry': self.phone_country,
            'Phone': self.phone,
            'Email': self.email,
            'Description': self.description,
            'FollowUps': self.followups,
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

@handle_exception
def delete_lead(query=None):
    lead = Database.delete(path='db/leads/uploaded', query=query)
    return lead