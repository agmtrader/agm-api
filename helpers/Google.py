from googleapiclient.discovery import build

from google.oauth2.credentials import Credentials

import base64
from email.message import EmailMessage

class Google:

  def __init__(self):
    self.Drive = self.GoogleDrive()
    self.Gmail = self.Gmail()

  class GoogleDrive:
    
    def __init__(self):
      self.service = self.authenticateGoogleDrive()

    def authenticateGoogleDrive(self):

      SCOPES = ["https://www.googleapis.com/auth/drive"]
      creds = Credentials.from_authorized_user_file("creds/AAAuthedToken.json", SCOPES)
      service = build('drive', 'v3', credentials=creds)
      return service

    def getSharedDriveInfo(self, drive_name):

      shared_drive = (
        self.service.drives()
        .list(
            q=f"name = '{drive_name}'",
            fields="nextPageToken, drives(id, name)"
      ).execute())['drives']

      return shared_drive[0]

    # Find folder's info using a parent's folder ID
    def getFolderInfo(self, parent_id, folder_name):

      folders = (
          self.service.files()
          .list(
              supportsAllDrives=True,
              includeItemsFromAllDrives=True,
              q=f"name = '{folder_name}' and '{parent_id}' in parents",
              fields="nextPageToken, files(id, name)",
          ).execute())['files']

      return folders[0]

    # Find file's info using its file name and it's parent folder
    def getFileInfo(self, parent_id, file_name):
      f = (
          self.service.files()
          .list(
              supportsAllDrives=True,
              includeItemsFromAllDrives=True,
              q=f"name = '{file_name}' and '{parent_id}' in parents",
              fields="nextPageToken, files(id, name)",
          ).execute())['files']

      return f[0]
    
  class Gmail:

    def __init__(self):
      # Authorize Gmail API with info@agmtechnology.com
      creds = Credentials.from_authorized_user_file('creds/GmailAuthedTokenInfo.json')
      self.service = build("gmail", "v1", credentials=creds)

    def sendClientEmail(self, data, client_email, subject):
      
      # Create email
      message = EmailMessage()

      message.set_content(data)

      message["To"] = client_email
      message["From"] = "info@agmtechnology.com"
      message["Bcc"] = "cr@agmtechnology.com,aa@agmtechnology.com,jc@agmtechnology.com,hc@agmtechnology.com, rc@agmtechnology.com"
      message["Subject"] = subject

      encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
      create_message = {"raw": encoded_message}

      # Send message
      send_message = (
          self.service.users()
          .messages()
          .send(userId="me", body=create_message)
          .execute()
      )

      return {'emailId':send_message["id"]}
