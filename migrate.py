import requests
from src.utils.connectors.firebase import Firebase
from src.utils.connectors.supabase import db

firebase = Firebase()

url = 'http://127.0.0.1:5000'

def access_api(endpoint, method='GET', data=None):
    try:
        auth = requests.post(
            url + '/token', 
            json={'token': 'bdb68ccc-213e-4a44-b481-b11d9d45da02', 'scopes': 'all'},
        )
        
        response = requests.request(
            method, 
            url + endpoint, 
            json=data, 
            headers={'Authorization': f'Bearer {auth.json()["access_token"]}'},
        )
        
        try:
            return response.json()
        except:
            return response.content
            
    except requests.exceptions.RequestException as e:
        raise

def migrate_users():
    users = firebase.read('users')
    unable_to_merge = []
    for user in users:
        new_user = {
            'name': user['name'],
            'email': user['email'],
            'image': user['image'],
            'scopes': user['scopes'],
            'password': user['password']
        }
        response = access_api('/users/create', 'POST', data={'user': new_user})
        print(response)

def migrate_contacts():
    contacts = firebase.read('db/clients/contacts')
    for contact in contacts:
        new_contact = {
            'country': contact['ContactCountry'],
            'name': contact['ContactName'],
            'company_name': contact['CompanyName'],
            'email': contact['ContactEmail'],
            'phone': contact['ContactPhone'],
        }
        response = access_api('/contacts/create', 'POST', data={'contact': new_contact})
        print(response)

def link_users_to_contacts():
    users = db.read('user', {})
    contacts = db.read('contact', {})
    for user in users:
        for contact in contacts:
            if user['email'] == contact['email']:
                contact['user_id'] = user['id']
                db.update('contact', {'id': contact['id']}, contact)
                break
            elif user['name'] == contact['name']:
                contact['user_id'] = user['id']
                db.update('contact', {'id': contact['id']}, contact)
                break

def migrate_advisors():
    contacts = db.read('contact', {})
    advisors = firebase.read('db/advisors/dictionary')

    for advisor in advisors:
        print("Name to find: " + advisor['AdvisorName'] + "\n") 
        for contact in contacts:
            if contact['name'] == advisor['AdvisorName']:
                advisor['contact_id'] = contact['id']
                break
            else:
                advisor['contact_id'] = None

        if advisor['contact_id'] is None:
            print('No contact found for advisor: ' + advisor['AdvisorName'] + '\n')
            new_contact = {
                'name': advisor['AdvisorName'],
            }
            advisor_id = db.create('contact', new_contact)
            advisor['contact_id'] = advisor_id

        new_advisor = {
            'name': advisor['AdvisorName'],
            'contact_id': advisor['contact_id'],
            'code': advisor['AdvisorCode'],
            'agency': advisor['Agency'],
            'hierarchy1': advisor['HierarchyL1'],
            'hierarchy2': advisor['HierarchyL2'],
        }
        
        advisor_id = db.create('advisor', new_advisor)
        print("Created advisor: " + advisor_id)

def migrate_leads():
    leads = firebase.read('db/clients/leads')
    old_contacts = firebase.read('db/clients/contacts')
    contacts = db.read('contact', {})

    for lead in leads:

        for old_contact in old_contacts:
            if old_contact['ContactID'] == lead['ContactID']:
                contact_name = old_contact['ContactName']
                for contact in contacts:
                    if contact['name'] == contact_name:
                        lead['ContactID'] = contact['id']
                        break
                    else:
                        lead['ContactID'] = None

        for old_contact in old_contacts:
            if old_contact['ContactID'] == lead['ReferrerID']:
                contact_name = old_contact['ContactName']
                print('Name to find: ' + contact_name + '\n')
                for contact in contacts:
                    if contact['name'] == contact_name:
                        lead['ReferrerID'] = contact['id']
                        break
                    else:
                        lead['ReferrerID'] = None
                
        if lead['ContactID'] is None:
            print('No contact found for lead: ' + lead['LeadID'] + '\n')
            continue

        if lead['ReferrerID'] is None:
            print('No referrer found for lead: ' + lead['LeadID'] + '\n')
            continue

        new_lead = {
            'contact_date': lead['ContactDate'],
            'contact_id': lead['ContactID'],
            'referrer_id': lead['ReferrerID'],
            'description': lead['Description'],
            'status': lead['Status'],
            'completed': lead['Completed'],
        }
        new_follow_ups = []
        for follow_up in lead['FollowUps']:
            new_follow_up = {
                'date': follow_up['date'],
                'description': follow_up['description'],
                'completed': follow_up['completed'],
            }
            new_follow_ups.append(new_follow_up)

        lead_id = access_api('/leads/create', 'POST', data={'lead': new_lead, 'follow_ups': new_follow_ups})

# Migrate users
#migrate_users()

# Migrate contacts
#migrate_contacts()

# Update user contact ids
link_users_to_contacts()

# Merge advisors
#migrate_advisors()

# Migrate leads
#migrate_leads()

# Migrate applications

# Migrate accounts

# Migrate documents