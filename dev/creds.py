"""
HELPER FOR TRANSFORMING GOOGLE CREDENTIALS INTO TOKENS
"""
import json
import os
from tempfile import NamedTemporaryFile
from google_auth_oauthlib.flow import InstalledAppFlow

def get_token(creds):
    SCOPES = ["https://mail.google.com/", "https://www.googleapis.com/auth/drive"]
    
    # Create a temporary file with the credentials
    with NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
        json.dump(creds, temp_file)
        temp_file.flush()  # Ensure all data is written
        temp_path = temp_file.name
    
    try:
        # Use the temporary file for the flow
        flow = InstalledAppFlow.from_client_secrets_file(temp_path, SCOPES)
        creds = flow.run_local_server(port=0)
        json_creds = creds.to_json()
        json_creds = json.loads(json_creds)
        return json_creds
    finally:
        # Ensure the temporary file is deleted even if an error occurs
        if os.path.exists(temp_path):
            os.remove(temp_path)