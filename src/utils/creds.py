"""
HELPER FOR TRANSFORMING GOOGLE CREDENTIALS INTO TOKENS
"""
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://mail.google.com/", "https://www.googleapis.com/auth/drive"]

flow = InstalledAppFlow.from_client_secrets_file(
        f"creds.json", SCOPES
)

creds = flow.run_local_server(port=0)

with open("token.json", "w") as token:
    token.write(creds.to_json())