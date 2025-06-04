import requests
import os
from src.utils.logger import logger
import json

from src.components.account_management import AccountManagement
account_management = AccountManagement()

url = f'http://127.0.0.1:{os.getenv("PORT")}'

def access_api(endpoint, method='GET', data=None):
    try:
        auth = requests.post(
            url + '/token', 
            json={'token': 'vIkY4of6iVgXRwLTMpHM', 'scopes': 'all'},
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
        logger.error(f"API request failed: {str(e)}")
        raise

external_id = "aguilarcarboni"
user_email = "aguilarcarboni@gmail.com"

application = {
    "application": {
        "customer": {
            "accountHolder": {
                "accountHolderDetails": [
                    {
                        "externalId": external_id,
                        "name": {
                            "first": "Andres",
                            "last": "Aguilar Carboni"
                        },
                        "email": user_email,
                        "residenceAddress": {
                            "country": "CRI",
                            "street1": "The Oasis",
                            "street2": "",
                            "city": "La Union",
                            "state": "CR-C",
                            "postalCode": "30301"
                        },
                        "countryOfBirth": "CRI",
                        "dateOfBirth": "2002-07-24",
                        "employmentDetails": {
                            "employer": "AGM Technology",
                            "occupation": "Software Engineer",
                            "employerBusiness": "Finance",
                            "employerAddress": {
                                "country": "CRI",
                                "street1": "Hype Way",
                                "street2": "",
                                "city": "Escazu",
                                "state": "CR-SJ",
                                "postalCode": "10301"
                            }
                        },
                        "employmentType": "EMPLOYED",
                        "phones": [
                            {
                                "type": "Mobile",
                                "country": "CRI",
                                "number": "83027366",
                                "verified": True
                            }
                        ],   
                        "sameMailAddress": True,
                        "identification": {
                            "passport": "118490741",
                            "issuingCountry": "CRI"
                        }
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
                                "usedForFunds": True,
                                "description": "Trading"
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
                                "status": True,
                                "details": "Affiliated with Interactive Brokers",
                                "detail": "Affiliated with Interactive Brokers",
                                "externalIndividualId": external_id
                            },
                            {
                                "code": "EmployeePubTrade",
                                "status": True,
                                "details": "Employee is not trading publicly",
                                "detail": "Employee is not trading publicly",
                                "externalIndividualId": external_id
                            },
                            {
                                "code": "ControlPubTraded",
                                "status": True,
                                "details": "Controlled trading is not allowed",
                                "detail": "Controlled trading is not allowed",
                                "externalIndividualId": external_id
                            }
                        ]
                    }
                ],
            },
            "externalId": external_id,
            "type": "INDIVIDUAL",
            "prefix": "damtes",
            "email": user_email,
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
            }
        ],
        "users": [
            {
                "externalUserId": external_id,
                "externalIndividualId": external_id,
                "prefix": "damtes"
            } 
        ],
    }
}

application = account_management.create_account(application)
for key, value in application.items():
    print(f"{key}: {value}")