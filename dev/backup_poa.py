from src.utils.api import access_api
import pandas as pd
import requests
from src.utils.logger import logger
from datetime import datetime

url = f'http://127.0.0.1:5000'

def access_api(endpoint, method='GET', data=None):
    try:
        # Add timeout to prevent hanging
        auth = requests.post(
            url + '/login', 
            json={'username': 'admin', 'password': 'password'},
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

file_content_response = access_api('/drive/export_file', 'POST', {'file_id': '1bKszsjPDijVqnPeuyVGCmGwre2nNAZzPf3lsD038sQ4', 'mime_type': 'text/csv', 'parse': True})
file_content = file_content_response['content']

df = pd.DataFrame(file_content)

def parse_date(date_str):
    if not date_str or date_str.isspace():
        return None
    
    try:
        # Try parsing dd/mm/yyyy format
        date_obj = datetime.strptime(date_str.strip(), '%d/%m/%Y')
    except ValueError:
        try:
            # Try parsing d/mm/yyyy format
            date_obj = datetime.strptime(date_str.strip(), '%-d/%m/%Y')
        except ValueError:
            return None
    
    # Convert to ISO format with UTC timezone
    return date_obj.strftime('%Y-%m-%dT06:00:00.000Z')

def parse_timestamp(timestamp_str):
    if not timestamp_str or timestamp_str.isspace():
        return None
    
    try:
        # Try parsing dd/mm/yyyy HH:MM:SS format
        date_obj = datetime.strptime(timestamp_str.strip(), '%d/%m/%Y %H:%M:%S')
    except ValueError:
        try:
            # Try parsing dd/mm/yyyy H:MM:SS format
            date_obj = datetime.strptime(timestamp_str.strip(), '%d/%m/%Y %H:%M:%S')
        except ValueError:
            try:
                # Try parsing mm/dd/yyyy HH:MM format (without seconds)
                date_obj = datetime.strptime(timestamp_str.strip(), '%m/%d/%Y %H:%M')
            except ValueError:
                return None
    
    return date_obj.strftime('%Y%m%d%H%M%S')

def extract_drive_id(url):
    if not url or not isinstance(url, str):
        return None
    
    # Handle drive.google.com/open?id= format
    if 'drive.google.com/open?id=' in url:
        return url.split('id=')[-1].split('&')[0]
    
    # Handle drive.google.com/file/d/ format
    if 'drive.google.com/file/d/' in url:
        return url.split('/file/d/')[-1].split('/')[0]
    
    # If the string looks like it's already a file ID, return it
    if len(url.strip()) > 25 and '/' not in url and '?' not in url:
        return url.strip()
        
    return None

for index, row in df.iterrows():
    timestamp = parse_timestamp(row["Marca temporal"])

    document_info = {
        'account_number': row['Account Number'],
        'issued_date': parse_date(row['Issued Date']),
        'type': row['Type']
    }

    file_id = extract_drive_id(row['Upload'])
    if not file_id:
        logger.warning(f"Could not extract file ID from: {row['Upload']}")
        continue
    
    response = access_api('/drive/get_file_info_by_id', 'POST', {'file_id': file_id})
    file_info = response['content']

    data = {
        'Category': 'poa',
        'DocumentInfo': document_info,
        'FileInfo': file_info,
        'Uploader':row['DirecciÃ³n de correo electrÃ³nico'],
        'DocumentID':timestamp
    }
    response = access_api('/database/create', 'POST', {
        'path':'db/document_center/poa',
        'data':data,
        'id':timestamp
    })
    if response['status'] != 'success':
        logger.warning(response)
        pass
    logger.info(data)
