"""
HELPER FOR TRANSFORMING GOOGLE CREDENTIALS INTO TOKENS
"""
import json
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://mail.google.com/", "https://www.googleapis.com/auth/drive"]

flow = InstalledAppFlow.from_client_secrets_file(
        f"creds.json", SCOPES
)

creds = flow.run_local_server(port=0)

translator = {
    'token':'GOOGLE_TOKEN',
    'refresh_token':'GOOGLE_REFRESH_TOKEN',
    'token_uri':'GOOGLE_TOKEN_URI',
    'client_id':'GOOGLE_CLIENT_ID',
    'client_secret':'GOOGLE_CLIENT_SECRET',
    'scopes':'GOOGLE_SCOPES',
    'universe_domain':'GOOGLE_UNIVERSE_DOMAIN',
    'account':'GOOGLE_ACCOUNT',
    'expiry':'GOOGLE_EXPIRY'
}

json_creds = creds.to_json()
json_creds = json.loads(json_creds)

with open("google.env", "w") as env_file:
    for key in json_creds.keys():
        env_file.write(f"{translator[key]}={json_creds[key]}")
        env_file.write('\n')