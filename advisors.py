import requests
import os
from src.utils.logger import logger
import pandas as pd
import random
import string
from datetime import datetime
import json
from difflib import SequenceMatcher
import time

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

advisors = access_api('/advisors/read', method='POST', data={'query': {}})
advisors_df = pd.DataFrame(advisors)
print(advisors_df)