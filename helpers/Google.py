from googleapiclient.discovery import build

from google.oauth2.credentials import Credentials

import base64
from email.message import EmailMessage

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

import time

import pandas as pd

class Google:
  
  class GoogleDrive:
    
    def __init__(self):
      
      SCOPES = ["https://www.googleapis.com/auth/drive"]
      creds = Credentials.from_authorized_user_file("creds/AAAuthedToken.json", SCOPES)
      self.service = build('drive', 'v3', credentials=creds)

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
    
    def getFilesInFolder(self, parent_id):

      files = (
          self.service.files()
          .list(
              supportsAllDrives=True,
              includeItemsFromAllDrives=True,
              q=f"'{parent_id}' in parents and trashed = false",
              fields="nextPageToken, files(id, name)",
          ).execute())['files']
      
      return files

    def uploadCSVFiles(self, files, parent_id):

      # Upload each report
      for f in files:

          df = pd.DataFrame([f])
          df.to_csv('temp.csv', index=False)

          # Upload batch file contents to server for new file
          media = MediaFileUpload('temp.csv', mimetype='text/csv')

          # Create new file metadata with properties of original file and new destination
          file_metadata = {
              'name': f['name'],
              'parents': [parent_id],
              'mimeType': 'text/csv'
          }

          # Check if file already exists

          # Create the new file in batch folder
          created_file = (
              self.service.files().create(
              supportsAllDrives=True,
              body=file_metadata,
              media_body=media,
              fields='id'
            )).execute()

          print('Stored file in batch:', f['name'])

      return {'status':'success'}

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

  class Firebase:

    def __init__(self):

      # Authenticate Firebase Credentials
      cred = credentials.Certificate('creds/FirebaseAdminSDK.json')

      try:
        firebase_admin.initialize_app(cred)
        print('Initialized Firebase connection.')
      except:
        print('App already exists.')

      # Secure connection to Firestore
      self.db = firestore.client()

    def getDocumentsFromCollection(self, path):

      # Read database
      users_ref = self.db.collection(path)
      docs = users_ref.stream()

      # Load data into dataframe
      clients = []

      for doc in docs:
        clients.append(doc.to_dict())

      return clients
    
    def addDataframeToCollection(self, df, path):

      for index, row in df.iterrows():
        info_dict = row.to_dict()
        self.addDocument(info_dict, path, f'{index}')

        if index == 0:
          print(f'Adding new collection.')

        elif index % 100 == 0:
          print(f'Added {index} documents.')

      print(f'Added {index} total documents.')

    def queryDocumentsFromCollection(self, path, key, value):

      # Read database
      ref = self.db.collection(path)

      # Create a query against the collection
      query = ref.where(filter=firestore.FieldFilter(key, "==", value))

      return query.stream()
    
    # TODO work on dependencies
    def deleteDocumentsFromCollection(self, path):
      users_ref = self.db.collection(path)
      docs = users_ref.stream()

      counter = 0
      for index, doc in enumerate(docs):
        self.db.collection(path).document(doc.id).delete()
        if index % 100 == 0:
          print(f'Deleted {index} documents.')
    
    def addDocument(self, data, path, id):

      self.db.collection(path).document(id).set(data)
