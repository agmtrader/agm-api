from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://mail.google.com/"]

date = '20241024'

flow = InstalledAppFlow.from_client_secrets_file(
        f"app/creds/oauth{date}.json", SCOPES
)

creds = flow.run_local_server(port=0)

with open("token.json", "w") as token:
    token.write(creds.to_json())