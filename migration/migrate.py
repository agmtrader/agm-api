import requests
from src.helpers.database import Firebase

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

def migrate_advisors():
    contacts = access_api('/contacts/read', 'POST', data={'query': {}})
    advisors = firebase.read('db/advisors/dictionary')

    for advisor in advisors[52:]:
        print("Name to find: " + advisor['AdvisorName'] + "\n") 
        for contact in contacts:
            if contact['name'] == advisor['AdvisorName']:
                advisor['ContactID'] = contact['id']
                break
            else:
                advisor['ContactID'] = None

        if advisor['ContactID'] is None:
            print('No contact found for advisor: ' + advisor['AdvisorName'] + '\n')
            new_contact = {
                'name': advisor['AdvisorName'],
            }
            contact_id = access_api('/contacts/create', 'POST', data={'contact': new_contact})
            advisor['ContactID'] = contact_id['id']

        new_advisor = {
            'name': advisor['AdvisorName'],
            'contact_id': advisor['ContactID'],
            'code': advisor['AdvisorCode'],
            'agency': advisor['Agency'],
            'hierarchy1': advisor['HierarchyL1'],
            'hierarchy2': advisor['HierarchyL2'],
        }
        
        advisor_id = access_api('/advisors/create', 'POST', data={'advisor': new_advisor})
        print("Created advisor: " + advisor_id['id'])

def migrate_leads():
    leads = firebase.read('db/clients/leads')
    old_contacts = firebase.read('db/clients/contacts')
    contacts = access_api('/contacts/read', 'POST', data={'query': {}})

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
