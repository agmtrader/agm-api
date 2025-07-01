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

def migrate_applications():
    # Read old tickets from API
    tickets = firebase.read('db/clients/tickets')
    
    if not tickets:
        print('No tickets found to migrate')
        return
    
    print(f'Found {len(tickets)} tickets to migrate')
    
    for ticket in tickets:
        try:
            # Extract ApplicationInfo from ticket
            app_info = ticket.get('ApplicationInfo', {})
            ticket_id = ticket.get('TicketID', '')
            
            if not app_info:
                print(f'No ApplicationInfo found for ticket {ticket_id}')
                continue
            
            # Map country codes to proper format
            def map_country_code(country_code):
                country_mapping = {
                    'cr': 'CRI',
                    'us': 'USA',
                    'ca': 'CAN',
                    # Add more mappings as needed
                }
                return country_mapping.get(country_code.lower(), country_code.upper())
            
            # Map marital status
            def map_marital_status(status):
                status_mapping = {
                    'Single': 'S',
                    'Married': 'M', 
                    'Divorced': 'D',
                    'Widowed': 'W',
                    'Separated': 'P'
                }
                return status_mapping.get(status, 'S')
            
            # Map employment status
            def map_employment_status(status):
                status_mapping = {
                    'Self-employed': 'SELF_EMPLOYED',
                    'Employed': 'EMPLOYED',
                    'Unemployed': 'UNEMPLOYED',
                    'Retired': 'RETIRED',
                    'Student': 'STUDENT'
                }
                return status_mapping.get(status, 'EMPLOYED')
            
            # Parse financial amounts (remove $ and commas, extract numbers)
            def parse_financial_amount(amount_str):
                if not amount_str:
                    return 0
                # Extract numeric values from ranges like "$150,000 - $499,999"
                import re
                numbers = re.findall(r'[\d,]+', str(amount_str).replace('$', ''))
                if numbers:
                    # Take the first number as a conservative estimate
                    return int(numbers[0].replace(',', ''))
                return 0
            
            # Map investment objectives
            def map_investment_objectives(objectives):
                if not objectives:
                    return ["Growth"]
                objective_mapping = {
                    'Profits': 'Trading',
                    'Growth': 'Growth',
                    'Income': 'Income',
                    'Preservation': 'Preservation',
                    'Capital': 'Growth',  # Capital appreciation maps to Growth
                    'Hedging': 'Trading'  # Hedging maps to Trading
                }
                return [objective_mapping.get(obj, obj) for obj in objectives]
            
            # Map products to trading permissions
            def map_trading_permissions(products):
                if not products:
                    return [{"country": "UNITED STATES", "product": "STOCKS"}]
                
                product_mapping = {
                    'Stocks': 'STOCKS',
                    'Bonds': 'BONDS', 
                    'Options': 'OPTIONS',
                    'Futures': 'FUTURES',
                    'ETFs': 'ETFS'
                }
                
                permissions = []
                for product in products:
                    mapped_product = product_mapping.get(product, product.upper())
                    permissions.append({
                        "country": "UNITED STATES",
                        "product": mapped_product
                    })
                return permissions
            
            # Map source of wealth
            def map_sources_of_wealth(sources):
                if not sources:
                    return [{"sourceType": "SOW-IND-Income", "percentage": 100, "usedForFunds": True}]
                
                source_mapping = {
                    'Property': 'SOW-IND-RealEstate',
                    'Salary': 'SOW-IND-Income',
                    'Business': 'SOW-IND-Business',
                    'Investment': 'SOW-IND-Investment',
                    'Income': 'SOW-IND-Income',
                    'Inheritance': 'SOW-IND-Inheritance',
                    'Profits': 'SOW-IND-Investment',  # Trading profits map to Investment
                    'Other': 'SOW-IND-Other'
                }
                
                mapped_sources = []
                percentage_per_source = 100 // len(sources)
                for i, source in enumerate(sources):
                    mapped_source = source_mapping.get(source, 'SOW-IND-Income')
                    percentage = percentage_per_source if i < len(sources) - 1 else 100 - (percentage_per_source * (len(sources) - 1))
                    mapped_sources.append({
                        "sourceType": mapped_source,
                        "percentage": percentage,
                        "usedForFunds": True
                    })
                return mapped_sources
            
            # Map account type
            def map_account_type(account_type):
                if not account_type:
                    return 'INDIVIDUAL'
                
                type_mapping = {
                    'Individual': 'INDIVIDUAL',
                    'Joint': 'JOINT',
                    'Corporate': 'CORPORATE',
                    'Trust': 'TRUST'
                }
                return type_mapping.get(account_type, account_type.upper())
            
            # Map salutation
            def map_salutation(salutation):
                if not salutation:
                    return 'Mr.'
                
                salutation_mapping = {
                    'Mr': 'Mr.',
                    'Mrs': 'Mrs.',
                    'Ms': 'Ms.',
                    'Dr': 'Dr.',
                    'Prof': 'Prof.'
                }
                return salutation_mapping.get(salutation, salutation if salutation.endswith('.') else salutation + '.')
            
            # Map investment experience based on objectives and products
            def map_investment_experience(objectives, products):
                # Base experience for stocks
                experiences = [{
                    "assetClass": "STK",
                    "yearsTrading": 1,
                    "tradesPerYear": 1,
                    "knowledgeLevel": "Good"
                }]
                
                # Add experience for other product types if they're requested
                if products:
                    product_to_asset_class = {
                        'Options': 'OPT',
                        'Futures': 'FUT',
                        'Bonds': 'BOND',
                        'ETFs': 'STK'  # ETFs typically use STK asset class
                    }
                    
                    for product in products:
                        if product in product_to_asset_class:
                            asset_class = product_to_asset_class[product]
                            # Only add if not already present
                            if not any(exp['assetClass'] == asset_class for exp in experiences):
                                experiences.append({
                                    "assetClass": asset_class,
                                    "yearsTrading": 1,
                                    "tradesPerYear": 1,
                                    "knowledgeLevel": "Good"
                                })
                
                return experiences
            
            # Create new application structure
            external_id = ticket_id
            prefix = "AGM" 
            
            new_application = {
                "customer": {
                    "accountHolder": {
                        "accountHolderDetails": [
                            {
                                "externalId": external_id,
                                "email": app_info.get('email', ''),
                                "name": {
                                    "first": app_info.get('first_name', ''),
                                    "last": app_info.get('last_name', ''),
                                    "salutation": map_salutation(app_info.get('salutation')),
                                },
                                "dateOfBirth": app_info.get('date_of_birth', '').split('T')[0] if app_info.get('date_of_birth') else '',
                                "countryOfBirth": map_country_code(app_info.get('country_of_birth', 'CRI')),
                                "numDependents": int(app_info.get('number_of_dependents', 0)) if app_info.get('number_of_dependents') else 0,
                                "maritalStatus": map_marital_status(app_info.get('marital_status', 'Single')),
                                "identification": {
                                    "passport": app_info.get('id_number', ''),
                                    "issuingCountry": map_country_code(app_info.get('id_country', 'CRI')),
                                    "expirationDate": app_info.get('id_expiration_date', '').split('T')[0] if app_info.get('id_expiration_date') else '',
                                    "citizenship": map_country_code(app_info.get('citizenship', 'CRI'))
                                },
                                "residenceAddress": {
                                    "country": map_country_code(app_info.get('country', 'CRI')),
                                    "street1": app_info.get('address', ''),
                                    "city": app_info.get('city', ''),
                                    "state": app_info.get('state', ''),
                                    "postalCode": app_info.get('zip', '')
                                },
                                "phones": [
                                    {
                                        "type": app_info.get('phone_type', 'Mobile'),
                                        "country": map_country_code(app_info.get('phone_country', 'CRI')),
                                        "number": app_info.get('phone_number', ''),
                                        "verified": True
                                    }
                                ],
                                "employmentType": map_employment_status(app_info.get('employment_status', 'Employed')),
                                "employmentDetails": {
                                    "employerBusiness": app_info.get('nature_of_business', ''),
                                    "employer": app_info.get('employer_name', ''),
                                    "occupation": app_info.get('occupation', ''),
                                    "employerAddress": {
                                        "country": map_country_code(app_info.get('employer_country', 'CRI')),
                                        "street1": app_info.get('employer_address', ''),
                                        "street2": "",
                                        "city": app_info.get('employer_city', ''),
                                        "state": app_info.get('employer_state', ''),
                                        "postalCode": app_info.get('employer_zip', '')
                                    }
                                },
                                "taxResidencies": [
                                    {
                                        "country": map_country_code(app_info.get('country', 'CRI')),
                                        "tin": app_info.get('tax_id', app_info.get('id_number', '')),
                                        "tinType": "NonUS_NationalId"
                                    }
                                ],
                                "w8Ben": {
                                    "localTaxForms": [],
                                    "name": f"{app_info.get('first_name', '')} {app_info.get('last_name', '')}",
                                    "foreignTaxId": app_info.get('tax_id', app_info.get('id_number', '')),
                                    "tinOrExplanationRequired": True,
                                    "part29ACountry": "N/A",
                                    "cert": True,
                                    "signatureType": "Electronic",
                                    "blankForm": True,
                                    "taxFormFile": "Form5001.pdf",
                                    "electronicFormat": True,
                                },
                                "gender": "M",  # Default, could be enhanced with mapping
                                "sameMailAddress": True,
                                "titles": [
                                    {
                                        "code": "Account Holder",
                                        "value": "Account Holder"
                                    }
                                ]
                            }
                        ],
                        "financialInformation": [
                            {
                                "investmentExperience": map_investment_experience(
                                    app_info.get('investment_objectives', []), 
                                    app_info.get('products', [])
                                ),
                                "investmentObjectives": map_investment_objectives(app_info.get('investment_objectives', [])),
                                "sourcesOfWealth": map_sources_of_wealth(app_info.get('source_of_wealth', [])),
                                "netWorth": parse_financial_amount(app_info.get('net_worth', '0')),
                                "liquidNetWorth": parse_financial_amount(app_info.get('liquid_net_worth', '0')),
                                "annualNetIncome": parse_financial_amount(app_info.get('annual_net_income', '0')),
                            }
                        ],
                        "regulatoryInformation": [
                            {
                                "regulatoryDetails": [
                                    {
                                        "code": "AFFILIATION",
                                        "status": False,
                                        "details": "Affiliated with Interactive Brokers",
                                        "externalIndividualId": external_id
                                    },
                                    {
                                        "code": "EmployeePubTrade",
                                        "status": False,
                                        "details": "Employee is not trading publicly",
                                        "externalIndividualId": external_id
                                    },
                                    {
                                        "code": "ControlPubTraded",
                                        "status": False,
                                        "details": "Controlled trading is not allowed",
                                        "externalIndividualId": external_id
                                    }
                                ]
                            }
                        ],
                    },
                    "externalId": external_id,
                    "type": map_account_type(app_info.get('account_type')),
                    "prefix": prefix,
                    "email": app_info.get('email', ''),
                    "mdStatusNonPro": True,
                    "meetAmlStandard": "true",
                    "directTradingAccess": True,
                    "legalResidenceCountry": map_country_code(app_info.get('country_of_residence', 'CRI'))
                },
                "accounts": [
                    {
                        "investmentObjectives": map_investment_objectives(app_info.get('investment_objectives', [])),
                        "tradingPermissions": map_trading_permissions(app_info.get('products', [])),
                        "externalId": external_id,
                        "baseCurrency": app_info.get('currency', 'USD'),
                        "multiCurrency": True,
                        "margin": "Cash",
                        "alias": "AGM"
                    },
                ],
                "users": [
                    {
                        "externalUserId": external_id,
                        "externalIndividualId": external_id,
                        "prefix": prefix
                    } 
                ],
                "documents": []
            }

            reference_application = {
                "customer": {
                    "accountHolder": {
                        "accountHolderDetails": [
                            {
                                "externalId": external_id,
                                "email": "test@test.com",
                                "name": {
                                    "first": "test",
                                    "last": "test",
                                    "salutation": "Mr.",
                                },
                                "dateOfBirth": "2002-07-24",
                                "countryOfBirth": "CRI",
                                "numDependents": 0,
                                "maritalStatus": "S",
                                "identification": {
                                    "passport": "118490741",
                                    "issuingCountry": "CRI",
                                    "expirationDate": "2030-07-24",
                                    "citizenship": "CRI"
                                },
                                "residenceAddress": {
                                    "country": "CRI",
                                    "street1": "Valle del Sol",
                                    "city": "San Jose",
                                    "state": "CR-SJ",
                                    "postalCode": "10301"
                                },
                                "phones": [
                                    {
                                        "type": "Mobile",
                                        "country": "CRI",
                                        "number": "83027366",
                                        "verified": True
                                    }
                                ],
                                "employmentType": "EMPLOYED",
                                "employmentDetails": {
                                    "employerBusiness": "Finance",
                                    "employer": "AGM Technology",
                                    "occupation": "Software Engineer",
                                    "employerAddress": {
                                        "country": "CRI",
                                        "street1": "Hype Way",
                                        "street2": "",
                                        "city": "Escazu",
                                        "state": "CR-SJ",
                                        "postalCode": "10301"
                                    }
                                },
                                "taxResidencies": [
                                    {
                                    "country": "CRI",
                                    "tin": "118490741",
                                    "tinType": "NonUS_NationalId"
                                    }
                                ],
                                "w8Ben": {
                                    "localTaxForms": [],
                                    "name": "test test",
                                    "foreignTaxId": "118490741",
                                    "tinOrExplanationRequired": True,
                                    "part29ACountry": "N/A",
                                    "cert": True,
                                    "signatureType": "Electronic",
                                    "blankForm": True,
                                    "taxFormFile": "Form5001.pdf",
                                    "electronicFormat": True,
                                },
                                "gender": "M",
                                "sameMailAddress": True,
                                "titles": [
                                    {
                                        "code": "Account Holder",
                                        "value": "Account Holder"
                                    }
                                ]
                            }
                        ],
                        "financialInformation": [
                            {
                                "investmentExperience": [
                                    {
                                        "assetClass": "STK",
                                        "yearsTrading": 1,
                                        "tradesPerYear": 1,
                                        "knowledgeLevel": "Good"
                                    }
                                ],
                                "investmentObjectives": [
                                    "Trading",
                                    "Growth"
                                ],
                                "sourcesOfWealth": [
                                    {
                                        "sourceType": 'SOW-IND-Income',
                                        "percentage": 100,
                                        "usedForFunds": True
                                    }
                                ],
                                "netWorth": 1000,
                                "liquidNetWorth": 1000,
                                "annualNetIncome": 1000,
                            }
                        ],
                        "regulatoryInformation": [
                            {
                                "regulatoryDetails": [
                                    {
                                        "code": "AFFILIATION",
                                        "status": False,
                                        "details": "Affiliated with Interactive Brokers",
                                        "externalIndividualId": external_id
                                    },
                                    {
                                        "code": "EmployeePubTrade",
                                        "status": False,
                                        "details": "Employee is not trading publicly",
                                        "externalIndividualId": external_id
                                    },
                                    {
                                        "code": "ControlPubTraded",
                                        "status": False,
                                        "details": "Controlled trading is not allowed",
                                        "externalIndividualId": external_id
                                    }
                                ]
                            }
                        ],
                    },
                    "externalId": external_id,
                    "type": "INDIVIDUAL",
                    "prefix": prefix,
                    "email": "test@test.com",
                    "mdStatusNonPro": True,
                    "meetAmlStandard": "true",
                    "directTradingAccess": True,
                    "legalResidenceCountry": "CRI"
                },
                "accounts": [
                    {
                        "investmentObjectives": [
                            "Trading",
                            "Growth"
                        ],
                        "tradingPermissions": [
                            {
                                "country": "UNITED STATES",
                                "product": "STOCKS"
                            }
                        ],
                        "externalId": external_id,
                        "baseCurrency": "USD",
                        "multiCurrency": True,
                        "margin": "Cash",
                        "alias": "AGM"
                    },
                ],
                "users": [
                    {
                        "externalUserId": external_id,
                        "externalIndividualId": external_id,
                        "prefix": prefix
                    } 
                ],
                "documents": [
                ]
            }

            # Make sure the new application has the same fields as the reference application, if not just throw error
            check_application_structure(new_application, reference_application)

            internal_application = {
                "advisor_id": None,
                "application": new_application,
                "date_sent_to_ibkr": None,
                "lead_id": None,
                "master_account_id": None,
                "user_id": None,
            }
            
            # Create application via API
            firebase.create(internal_application, 'applications')
            
        except Exception as e:
            print(f'Error migrating ticket {ticket.get("TicketID", "unknown")}: {str(e)}')
            continue

def check_application_structure(new_app, reference_app, path=""):
    """
    Recursively check if two applications have the same structure.
    Throws an error if structures differ, showing the exact field path that differs.
    """
    def get_structure_type(obj):
        """Get the type and structure info of an object"""
        if isinstance(obj, dict):
            return 'dict'
        elif isinstance(obj, list):
            return 'list'
        else:
            return type(obj).__name__
    
    def compare_dict_structure(dict1, dict2, current_path):
        """Compare two dictionaries' structure"""
        keys1 = set(dict1.keys())
        keys2 = set(dict2.keys())
        
        # Check for missing keys in either direction
        missing_in_new = keys2 - keys1
        missing_in_reference = keys1 - keys2
        
        if missing_in_new:
            raise ValueError(f"Field missing in new application: {current_path}.{next(iter(missing_in_new))}")
        
        if missing_in_reference:
            raise ValueError(f"Extra field in new application: {current_path}.{next(iter(missing_in_reference))}")
        
        # Recursively check common keys
        for key in keys1:
            new_path = f"{current_path}.{key}" if current_path else key
            compare_structures(dict1[key], dict2[key], new_path)
    
    def compare_list_structure(list1, list2, current_path):
        """Compare two lists' structure"""
        if len(list1) == 0 and len(list2) == 0:
            return
        
        # For non-empty lists, compare the structure of the first element
        # This assumes all elements in the list have the same structure
        if len(list1) > 0 and len(list2) > 0:
            compare_structures(list1[0], list2[0], f"{current_path}[0]")
        elif len(list1) == 0 and len(list2) > 0:
            raise ValueError(f"List is empty in new application but has elements in reference: {current_path}")
        elif len(list1) > 0 and len(list2) == 0:
            raise ValueError(f"List has elements in new application but is empty in reference: {current_path}")
    
    def compare_structures(obj1, obj2, current_path):
        """Main comparison function"""
        type1 = get_structure_type(obj1)
        type2 = get_structure_type(obj2)
        
        # Check if types match
        if type1 != type2:
            raise ValueError(f"Type mismatch at {current_path}: new application has {type1}, reference has {type2}")
        
        # Compare based on type
        if isinstance(obj1, dict):
            compare_dict_structure(obj1, obj2, current_path)
        elif isinstance(obj1, list):
            compare_list_structure(obj1, obj2, current_path)
        # For primitive types, we don't need to check further
    
    # Start the comparison
    try:
        compare_structures(new_app, reference_app, path)
        print("✓ Application structure matches reference application")
    except ValueError as e:
        raise ValueError(f"Application structure validation failed: {str(e)}")

def migrate_accounts():
    # Read accounts from Firebase
    accounts = firebase.read('db/clients/accounts')
    
    if not accounts:
        print('No accounts found to migrate')
        return
    
    print(f'Found {len(accounts)} accounts to migrate')
    
    # Read applications from Firebase to match with accounts
    applications = firebase.read('applications')
    
    if not applications:
        print('No applications found to match accounts with')
        return
    
    # Read users from database to match accounts by email
    users = db.read('user', {})
    user_email_mapping = {}
    default_user_id = None
    
    if users:
        for user in users:
            email = user.get('email', '').lower()
            if email:
                user_email_mapping[email] = user['id']
        default_user_id = users[0]['id']  # Use first user as default
        print(f'Created user mapping for {len(user_email_mapping)} users')
        print(f'Using default user {default_user_id} for unmatched accounts')
    else:
        print('No users found in database. Please migrate users first.')
        return
    
    # Create a mapping of externalId to application_id for quick lookup
    app_mapping = {}
    for app in applications:
        # Extract externalId from the application JSON structure
        try:
            external_id = app['application']['customer']['externalId']
            app_mapping[external_id] = app['id']
        except (KeyError, TypeError) as e:
            print(f'Could not extract externalId from application {app.get("id", "unknown")}: {e}')
            continue
    
    print(f'Created mapping for {len(app_mapping)} applications')
    
    for account in accounts:
        try:
            ticket_id = account.get('TicketID', '')
            account_id = account.get('AccountID', '')
            
            if not ticket_id:
                print(f'No TicketID found for account {account_id}')
                continue
            
            # Find matching application using TicketID as externalId
            application_id = app_mapping.get(ticket_id)
            
            if not application_id:
                print(f'No matching application found for account {account_id} with TicketID {ticket_id}')
                continue
            
            # Try to find matching user by temporal email
            temporal_email = account.get('TemporalEmail', '').lower()
            user_id = None
            if temporal_email and temporal_email in user_email_mapping:
                user_id = user_email_mapping[temporal_email]
                print(f'Found matching user {user_id} for email {temporal_email}')
            else:
                user_id = default_user_id
                print(f'No matching user found for email {temporal_email}, using default user {default_user_id}')
            
            # Create new account structure
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
            
            new_account = {
                'application_id': application_id,
                'ibkr_account_number': account.get('AccountNumber', ''),
                'ibkr_username': account.get('IBKRUsername', ''),
                'ibkr_password': account.get('IBKRPassword', ''),
                'temporal_email': account.get('TemporalEmail', ''),
                'temporal_password': account.get('TemporalPassword', ''),
                'fee_template': None,  # Default fee template
                'user_id': None,
                'created': now,
                'updated': now
            }
            
            # Create account in database
            account_db_id = db.create('account', new_account)
            print(f'Created account {account_db_id} for application {application_id} (IBKR Account: {account.get("AccountNumber", "N/A")})')
            
            # Update the application's master_account_id in Firebase (only if it's not already set)
            current_app = None
            for app in applications:
                if app.get('id') == application_id:
                    current_app = app
                    break
            
            if current_app and not current_app.get('master_account_id'):
                # Update in Firebase
                firebase.update({'master_account_id': account_db_id}, f'applications/{application_id}', query_params={'id': application_id})
                print(f'Set master_account_id for application {application_id}')
            
        except Exception as e:
            print(f'Error migrating account {account.get("AccountID", "unknown")}: {str(e)}')
            continue
    
    print('Account migration completed')

def update_application_status():
    """
    Update applications with status from tickets.
    Match applications by externalID to tickets by TicketID.
    """
    print('Starting application status update...')
    
    # Read applications from Firebase
    applications = firebase.read('applications')
    if not applications:
        print('No applications found')
        return
    
    # Read tickets from Firebase
    tickets = firebase.read('db/clients/tickets')
    if not tickets:
        print('No tickets found')
        return
    
    print(f'Found {len(applications)} applications and {len(tickets)} tickets')
    
    # Create ticket mapping by TicketID
    ticket_mapping = {}
    for ticket in tickets:
        ticket_id = ticket.get('TicketID')
        if ticket_id:
            ticket_mapping[ticket_id] = ticket
    
    print(f'Created mapping for {len(ticket_mapping)} tickets')
    
    updated_count = 0
    for application in applications:
        try:
            # Extract externalID from application structure
            external_id = application['application']['customer']['accountHolder']['accountHolderDetails'][0]['externalId']
            
            # Find matching ticket
            if external_id in ticket_mapping:
                ticket = ticket_mapping[external_id]
                ticket_status = ticket.get('Status', 'Unknown')
                
                # Update application with status
                application_id = application.get('id')
                if application_id:
                    firebase.update(f'applications', {'status': ticket_status}, query={'id': application_id})
                    print(f'Updated application {application_id} with status: {ticket_status}')
                    updated_count += 1
                else:
                    print(f'No application ID found for external ID {external_id}')
            else:
                print(f'No matching ticket found for external ID {external_id}')
                
        except (KeyError, IndexError, TypeError) as e:
            print(f'Error processing application {application.get("id", "unknown")}: {e}')
            continue
    
    print(f'Application status update completed. Updated {updated_count} applications.')

# Migrate users
#migrate_users()

# Migrate contacts
#migrate_contacts()

# Update user contact ids
#link_users_to_contacts()

# Merge advisors
#migrate_advisors()

# Migrate leads
#migrate_leads()

# Migrate applications
#migrate_applications()

# Migrate accounts
#migrate_accounts()

# Update application status from tickets
update_application_status()

# Migrate documents