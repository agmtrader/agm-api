from googleapiclient.discovery import build

from google.oauth2.credentials import Credentials

import base64
from email.message import EmailMessage

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

import pandas as pd

from app.helpers.logger import logger
from app.helpers.response import Response


class GoogleDrive:
  
  def __init__(self):
    try:
      SCOPES = ["https://www.googleapis.com/auth/drive"]
      creds = Credentials.from_authorized_user_file("app/creds/AAAuthedToken.json", SCOPES)
      self.service = build('drive', 'v3', credentials=creds)
    except Exception as e:
      logger.error(f"Error initializing GoogleDrive: {str(e)}")

  def getSharedDriveInfo(self, drive_name):
    try:
      shared_drive = (
        self.service.drives()
        .list(
            q=f"name = '{drive_name}'",
            fields="nextPageToken, drives(id, name)"
      ).execute())['drives']

      if not shared_drive:
        return Response.error(f"No shared drive found with name '{drive_name}'")
      return Response.success(shared_drive[0])
    except Exception as e:
      return Response.error(f"Error retrieving shared drive info: {str(e)}")

  # Find folder's info using a parent's folder ID
  def getFolderInfo(self, parent_id, folder_name):
    try:
      folders = (
          self.service.files()
          .list(
              supportsAllDrives=True,
              includeItemsFromAllDrives=True,
              q=f"name = '{folder_name}' and '{parent_id}' in parents",
              fields="nextPageToken, files(id, name, parents)",
          ).execute())['files']

      if not folders:
        return Response.error(f"No folder found with name '{folder_name}' in parent '{parent_id}'")
      return Response.success(folders[0])
    except Exception as e:
      return Response.error(f"Error retrieving folder info: {str(e)}")

  # Find file's info using its file name and it's parent folder
  def getFileInfo(self, parent_id, file_name):
    try:
      f = (
          self.service.files()
          .list(
              supportsAllDrives=True,
              includeItemsFromAllDrives=True,
              q=f"name = '{file_name}' and '{parent_id}' in parents",
              fields="nextPageToken, files(id, name, parents)",
          ).execute())['files']

      if not f:
        return Response.error(f"No file found with name '{file_name}' in parent '{parent_id}'")
      return Response.success(f[0])
    except Exception as e:
      return Response.error(f"Error retrieving file info: {str(e)}")
  
  def getFilesInFolder(self, parent_id):
    try:
      files = (
          self.service.files()
          .list(
              supportsAllDrives=True,
              includeItemsFromAllDrives=True,
              q=f"'{parent_id}' in parents and trashed = false",
              fields="nextPageToken, files(id, name, parents)",
          ).execute())['files']
      
      return Response.success(files)
    except Exception as e:
      return Response.error(f"Error retrieving files in folder: {str(e)}")

  def uploadCSVFiles(self, files, parent_id):
    try:
      logger.info(f'Uploading files: {list(files.keys())}')

      for file_name in list(files.keys()):
        df = pd.DataFrame(files[file_name])
        df.to_csv('cache/temp.csv', index=False)

        media = MediaFileUpload('cache/temp.csv', mimetype='text/csv')

        file_metadata = {
            'name': file_name,
            'parents': [parent_id],
            'mimeType': 'text/csv'
        }

        created_file = (
            self.service.files().create(
            supportsAllDrives=True,
            body=file_metadata,
            media_body=media,
            fields='id, name, parents'
          )).execute()

        logger.success(f'Stored file: {created_file}')

      return Response.success('Files uploaded successfully')
    except Exception as e:
      return Response.error(f"Error uploading CSV files: {str(e)}")
  
  def renameFiles(self, files):
    try:
      print('Renaming files: ', files)

      for f in files:
        file_metadata = {
          'name': f['new_name']
        }

        renamedFile = (
          self.service.files().update(
            fileId=f['id'],
            body=file_metadata,
            supportsAllDrives=True,
        )).execute()
        
      print('Done.')
      return Response.success(files)
    except Exception as e:
      return Response.error(f"Error renaming files: {str(e)}")

  def moveFile(self, f, new_parent_id):
    try:
      print('Moving file:', f)
      
      updated_file = self.service.files().update(
          fileId=f['id'],
          removeParents=f['parents'][0],
          addParents=new_parent_id,
          fields='id, parents, name',
          supportsAllDrives=True
      ).execute()

      return Response.success(updated_file)
    except Exception as e:
      return Response.error(f"Error moving file: {str(e)}")

class Gmail:

  def __init__(self):
    try:
      creds = Credentials.from_authorized_user_file('creds/GmailAuthedTokenInfo.json')
      self.service = build("gmail", "v1", credentials=creds)
    except Exception as e:
      logger.error(f"Error initializing Gmail: {str(e)}")

  def sendClientEmail(self, data, client_email, subject):
    try:
      message = EmailMessage()
      message.set_content(data)
      message["To"] = client_email
      message["From"] = "info@agmtechnology.com"
      message["Bcc"] = "cr@agmtechnology.com,aa@agmtechnology.com,jc@agmtechnology.com,hc@agmtechnology.com, rc@agmtechnology.com"
      message["Subject"] = subject

      encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
      create_message = {"raw": encoded_message}

      send_message = (
          self.service.users()
          .messages()
          .send(userId="me", body=create_message)
          .execute()
      )

      return Response.success({'emailId': send_message["id"]})
    except Exception as e:
      return Response.error(f"Error sending client email: {str(e)}")

class Firebase:

  def __init__(self):
    try:
      cred = credentials.Certificate('creds/FirebaseAdminSDK.json')
      firebase_admin.initialize_app(cred)
      print('Initialized Firebase connection.')
      self.db = firestore.client()
    except ValueError:
      print('App already exists.')
      self.db = firestore.client()
    except Exception as e:
      logger.error(f"Error initializing Firebase: {str(e)}")

  def getDocumentsFromCollection(self, path):
    try:
      users_ref = self.db.collection(path)
      docs = users_ref.stream()

      clients = []
      for doc in docs:
        clients.append(doc.to_dict())

      return Response.success(clients)
    except Exception as e:
      return Response.error(f"Error getting documents from collection: {str(e)}")
  
  def addDataframeToCollection(self, df, path):
    try:
      for index, row in df.iterrows():
        info_dict = row.to_dict()
        self.addDocument(info_dict, path, f'{index}')

        if index == 0:
          print(f'Adding new collection.')
        elif index % 100 == 0:
          print(f'Added {index} documents.')

      print(f'Added {index} total documents.')
      return Response.success(f'Added {index} total documents.')
    except Exception as e:
      return Response.error(f"Error adding dataframe to collection: {str(e)}")

  def queryDocumentsInCollection(self, path, key, value):
    try:
      ref = self.db.collection(path)
      query = ref.where(filter=firestore.FieldFilter(key, "==", value))
      return Response.success(query.stream())
    except Exception as e:
      return Response.error(f"Error querying documents from collection: {str(e)}")
  
  def deleteDocumentsFromCollection(self, path):
    try:
      users_ref = self.db.collection(path)
      docs = users_ref.stream()

      counter = 0
      for index, doc in enumerate(docs):
        self.db.collection(path).document(doc.id).delete()
        if index % 100 == 0:
          print(f'Deleted {index} documents.')
      
      return Response.success(f'Deleted {index} documents.')
    except Exception as e:
      return Response.error(f"Error deleting documents from collection: {str(e)}")
  
  def addDocument(self, data, path, id):
    try:
      self.db.collection(path).document(id).set(data)
      return Response.success(f'Document added successfully.')
    except Exception as e:
      return Response.error(f"Error adding document: {str(e)}")
