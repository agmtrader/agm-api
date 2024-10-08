import os
import io
import base64
import pandas as pd

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from app.helpers.logger import logger
from app.helpers.response import Response

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload, MediaIoBaseUpload

from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from jinja2 import Environment, FileSystemLoader
from premailer import transform

class GoogleDrive:
  
  def __init__(self):
    logger.info('Initializing GoogleDrive connection.')
    try:
      SCOPES = ["https://www.googleapis.com/auth/drive"]
      creds = Credentials(
        token=os.getenv('TOKEN'),
        refresh_token=os.getenv('REFRESH_TOKEN'),
        token_uri=os.getenv('TOKEN_URI'),
        client_id=os.getenv('CLIENT_ID'),
        client_secret=os.getenv('CLIENT_SECRET'),
        scopes=SCOPES
      )
      self.service = build('drive', 'v3', credentials=creds)
      logger.success('Initialized GoogleDrive connection.')
    except Exception as e:
      logger.error(f"Error initializing GoogleDrive: {str(e)}")

  def getSharedDriveInfo(self, drive_name):
    logger.info(f'Getting shared drive info for drive: {drive_name}')
    try:
      shared_drive = (
        self.service.drives()
        .list(
            q=f"name = '{drive_name}'",
            fields="nextPageToken, drives(id, name)"
      ).execute())['drives']

      if not shared_drive:
        logger.error(f"No shared drive found with name '{drive_name}'")
        return Response.error(f"No shared drive found with name '{drive_name}'")
      logger.success(f"Shared drive found with name '{drive_name}'")
      return Response.success(shared_drive[0])
    except Exception as e:
      logger.error(f"Error retrieving shared drive info: {str(e)}")
      return Response.error(f"Error retrieving shared drive info: {str(e)}")

  def getFolderInfo(self, parent_id, folder_name):
    logger.info(f'Getting folder info for folder: {folder_name} in parent: {parent_id}')
    try:
      folders = (
          self.service.files()
          .list(
              supportsAllDrives=True,
              includeItemsFromAllDrives=True,
              q=f"name = '{folder_name}' and '{parent_id}' in parents and trashed = false",
              fields="nextPageToken, files(id, name, parents)",
          ).execute())['files']

      if not folders:
        logger.error(f"No folder found with name '{folder_name}' in parent '{parent_id}'")
        return Response.error(f"No folder found with name '{folder_name}' in parent '{parent_id}'")
      logger.success(f"Folder found with name '{folder_name}' in parent '{parent_id}'")
      return Response.success(folders[0])
    except Exception as e:
      logger.error(f"Error retrieving folder info: {str(e)}")
      return Response.error(f"Error retrieving folder info: {str(e)}")

  def getFilesInFolder(self, parent_id):
    logger.info(f'Getting files in folder: {parent_id}')
    try:
      files = (
          self.service.files()
          .list(
              supportsAllDrives=True,
              includeItemsFromAllDrives=True,
              q=f"'{parent_id}' in parents and trashed = false",
              fields="nextPageToken, files(id, name, parents, mimeType, size, modifiedTime, createdTime)",
          ).execute())['files']
      
      logger.success(f'Files found in folder: {parent_id}')
      return Response.success(files)
    except Exception as e:
      logger.error(f"Error retrieving files in folder: {str(e)}")
      return Response.error(f"Error retrieving files in folder: {str(e)}")


  def getFileInfo(self, parent_id, file_name):
    logger.info(f'Getting file info for file: {file_name} in parent: {parent_id}')
    try:
      f = (
          self.service.files()
          .list(
              supportsAllDrives=True,
              includeItemsFromAllDrives=True,
              q=f"name = '{file_name}' and '{parent_id}' in parents and trashed = false",
              fields="nextPageToken, files(id, name, parents)",
          ).execute())['files']

      if not f:
        logger.error(f"No file found with name '{file_name}' in parent '{parent_id}'")
        return Response.error(f"No file found with name '{file_name}' in parent '{parent_id}'")
      logger.success(f"File found with name '{file_name}' in parent '{parent_id}'")
      return Response.success(f[0])
    except Exception as e:
      logger.error(f"Error retrieving file info: {str(e)}")
      return Response.error(f"Error retrieving file info: {str(e)}")
  
  def uploadCSVFiles(self, files, parent_id):
    logger.info(f'Uploading files: {list(files.keys())} to folder: {parent_id}')
    try:

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

      logger.success('Files uploaded successfully')
      return Response.success('Files uploaded successfully')
    
    except Exception as e:
      logger.error(f"Error uploading CSV files: {str(e)}")
      return Response.error(f"Error uploading CSV files: {str(e)}")
  
  def renameFiles(self, files):
    try:

      for f in files:

        logger.info(f'Renaming file: {f}')
        file_metadata = {
          'name': f['new_name']
        }

        renamedFile = (
          self.service.files().update(
            fileId=f['id'],
            body=file_metadata,
            supportsAllDrives=True,
        )).execute()

        logger.success(f'Successfully renamed file: {f}')
        
      logger.success('Successfully renamed files.')
      return Response.success(files)
    except Exception as e:
      logger.error(f"Error renaming files: {str(e)}")
      return Response.error(f"Error renaming files: {str(e)}")

  def moveFile(self, f, new_parent_id):
    logger.info(f'Moving file: {f} to new parent: {new_parent_id}')
    try:
      
      updated_file = self.service.files().update(
          fileId=f['id'],
          removeParents=f['parents'][0],
          addParents=new_parent_id,
          fields='id, parents, name',
          supportsAllDrives=True
      ).execute()

      logger.success(f'Successfully moved file: {f}')
      return Response.success(updated_file)
    except Exception as e:
      logger.error(f"Error moving file: {str(e)}")
      return Response.error(f"Error moving file: {str(e)}")

  def createFolder(self, folderName, parentFolderId):

      logger.info(f"Creating folder: {folderName} in folder: {parentFolderId}")

      fileMetadata = {
          'name': folderName,
          'mimeType': 'application/vnd.google-apps.folder'
      }
      if parentFolderId is not None:
          fileMetadata['parents'] = [parentFolderId]
      else:
          logger.error("No parent folder ID provided.")
          return Response.error('No parent folder ID provided.')
      
      folder = self.service.files().create(body=fileMetadata, fields='id, name, parents, mimeType, size, modifiedTime, createdTime').execute()
      logger.success(f"Successfully created folder: {folderName} in folder: {parentFolderId}")
      return Response.success(folder)

  def uploadFile(self, fileName, mimeType, rawFile, parentFolderId):
      logger.info(f"Uploading file: {fileName} to folder: {parentFolderId}")
      fileMetadata = {'name': fileName, 'mimeType': mimeType}

      if parentFolderId is not None:
          fileMetadata['parents'] = [parentFolderId]
      else:
          logger.error("No parent folder ID provided.")
          return Response.error('No parent folder ID provided.')
      try:
          media = MediaIoBaseUpload(rawFile, resumable=True, mimetype=mimeType)
          f = self.service.files().create(body=fileMetadata, media_body=media, fields='id, name, parents, mimeType, size, modifiedTime').execute()
          logger.success(f"Successfully uploaded file: {fileName} to folder: {parentFolderId}")
          return Response.success(f)
      except Exception as e:
          logger.error(f"Error uploading file: {fileName}. Error: {str(e)}")
          return Response.error(f'Error uploading file: {str(e)}')

  def deleteFiles(self, file_ids):

      logger.info(f"Deleting files with IDs: {file_ids}")

      results = []
      for file_id in file_ids:
          try:
              response = self.service.files().delete(fileId=file_id).execute()
              logger.success(f"Successfully deleted file with ID: {file_id}")
              results.append(Response.success({'content': response, 'file_id': file_id}))
          except Exception as e:
              logger.error(f"Error deleting file with ID: {file_id}. Error: {str(e)}")
              results.append(Response.error({'content': f'Error deleting file: {str(e)}', 'file_id': file_id}))

      logger.success(f"Deletion process completed for {len(file_ids)} files.")
      return results  

  def downloadFile(self, fileId):

    logger.info(f"Downloading file with ID: {fileId}")

    try:
        request = self.service.files().get_media(fileId=fileId)
        downloaded_file = io.BytesIO()
        downloader = MediaIoBaseDownload(downloaded_file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logger.info(f"Download {int(status.progress() * 100)}.")

    except HttpError as error:
        logger.error(f"An error occurred: {error}")
        return Response.error(error)
    
    except:
        logger.error("Error downloading file.")
        return Response.error('Error downloading file.')
    
    logger.success("Successfully downloaded file.")
    return Response.success(downloaded_file.getvalue())

class Gmail:

  def __init__(self):
    logger.info('Initializing Gmail connection.')
    try:
      creds = Credentials(
        token=os.getenv('INFO_TOKEN'),
        refresh_token=os.getenv('INFO_REFRESH_TOKEN'),
        token_uri=os.getenv('INFO_TOKEN_URI'),
        client_id=os.getenv('INFO_CLIENT_ID'),
        client_secret=os.getenv('INFO_CLIENT_SECRET'),
        scopes=os.getenv('INFO_SCOPES').split(',')
      )
      self.service = build("gmail", "v1", credentials=creds)
      logger.success('Initialized Gmail connection.')
    except Exception as e:
      logger.error(f"Error initializing Gmail: {str(e)}")

  def create_html_email(self, plain_text, subject):

    # Load the HTML template
    env = Environment(loader=FileSystemLoader('app/helpers/email_templates'))
    template = env.get_template('trade_ticket.html')

    # Render the template with the plain text content
    html_content = template.render(content=plain_text, subject=subject)

    # Inline the CSS
    html_content_inlined = transform(html_content)

    # Create a multipart message
    message = MIMEMultipart('related')
    message['Subject'] = subject
    message['From'] = "info@agmtechnology.com"
    message['To'] = "recipient@example.com"

    # Attach the HTML content
    message.attach(MIMEText(html_content_inlined, 'html'))

    # Attach the logo image
    logo_path = 'app/assets/agm-logo.png'
    with open(logo_path, 'rb') as logo_file:
        logo_mime = MIMEImage(logo_file.read())
        logo_mime.add_header('Content-ID', '<logo>')
        message.attach(logo_mime)

    return message

  def sendClientEmail(self, plain_text, client_email, subject):
    try:
        message = self.create_html_email(plain_text, subject)
        del message['To']  # Remove the 'To' field set in create_html_email
        message['To'] = client_email  # Set the correct 'To' field

        message['Bcc'] = "cr@agmtechnology.com,aa@agmtechnology.com,jc@agmtechnology.com,hc@agmtechnology.com,rc@agmtechnology.com"

        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = {"raw": raw_message}

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
    logger.info('Initializing Firebase connection.')
    try:
      cred = credentials.Certificate({
          "type": os.getenv('FIREBASE_TYPE'),
          "project_id": os.getenv('FIREBASE_PROJECT_ID'),
          "private_key_id": os.getenv('FIREBASE_PRIVATE_KEY_ID'),
          "private_key": os.getenv('FIREBASE_PRIVATE_KEY').replace('"', '').replace('\\n', '\n').replace(',', ''),
          "client_email": os.getenv('FIREBASE_CLIENT_EMAIL'),
          "client_id": os.getenv('FIREBASE_CLIENT_ID'),
          "auth_uri": os.getenv('FIREBASE_AUTH_URI'),
          "token_uri": os.getenv('FIREBASE_TOKEN_URI'),
          "auth_provider_x509_cert_url": os.getenv('FIREBASE_AUTH_PROVIDER_X509_CERT_URL'),
          "client_x509_cert_url": os.getenv('FIREBASE_CLIENT_X509_CERT_URL'),
          "universe_domain": os.getenv('FIREBASE_UNIVERSE_DOMAIN')
      })
      firebase_admin.initialize_app(cred)
      logger.success('Initialized Firebase connection.')
      self.db = firestore.client()
    except ValueError:
      logger.info('App already exists.')
      self.db = firestore.client()
    except Exception as e:
      logger.error(f"Error initializing Firebase: {str(e)}")

  def clear_collection(self, path):
    logger.info(f'Clearing collection: {path}')
    try:
      self.db.collection(path).delete()
      logger.success(f'Collection cleared successfully.')
      return Response.success(f'Collection cleared successfully.')
    except Exception as e:
      logger.error(f"Error clearing collection: {str(e)}")
      return Response.error(f"Error clearing collection: {str(e)}")

  def read_all(self, path):
    try:
      users_ref = self.db.collection(path)
      docs = users_ref.stream()

      clients = []
      for doc in docs:
        clients.append(doc.to_dict())

      return Response.success(clients)
    except Exception as e:
      return Response.error(f"Error getting documents from collection: {str(e)}")
  
  def upload_collection(self, data_array, path):
    try:
      # Clear the collection before uploading
      self.clearCollection(path)
    except Exception as e:
      return Response.error(f"Error clearing collection: {str(e)}")
    
    try:
      for index, info_dict in enumerate(data_array):
        self.create(info_dict, path, f'{index}')

        if index == 0:
          print(f'Adding new collection.')
        elif index % 100 == 0:
          print(f'Added {index + 1} documents.')

      print(f'Added {len(data_array)} total documents.')
      return Response.success(f'Added {len(data_array)} total documents.')
    except Exception as e:
      return Response.error(f"Error adding data to collection: {str(e)}")
    
  def read(self, path, query=None):
    logger.info(f'Querying documents in collection: {path} with query: {query}')
    try:
      if not path:
        raise ValueError("Path cannot be empty")
      
      ref = self.db.collection(path)
      if query:
        if not isinstance(query, dict):
          raise TypeError("Query must be a dictionary")
        for key, value in query.items():
          if not isinstance(key, str):
            raise TypeError("Query keys must be strings")
          ref = ref.where(filter=firestore.FieldFilter(key, "==", value))
      
      logger.success(f'Successfully queried documents.')
      results = []
      for doc in ref.stream():
          doc_dict = doc.to_dict()
          doc_dict['id'] = doc.id  # Add the document ID to the dictionary
          results.append(doc_dict)
      
      logger.info(f'Retrieved {len(results)} documents.')
      return Response.success(results)
    except ValueError as ve:
      logger.error(f"Value error in read operation: {str(ve)}")
      return Response.error(f"Value error in read operation: {str(ve)}")
    except TypeError as te:
      logger.error(f"Type error in read operation: {str(te)}")
      return Response.error(f"Type error in read operation: {str(te)}")
    except Exception as e:
      logger.error(f"Error querying documents from collection: {str(e)}")
      return Response.error(f"Error querying documents from collection: {str(e)}")
  
  def delete(self, path):
    logger.info(f'Deleting documents in collection: {path}')
    try:
      if not path:
        raise ValueError("Path cannot be empty")
      
      users_ref = self.db.collection(path)
      docs = users_ref.stream()

      deleted_count = 0
      for doc in docs:
        self.db.collection(path).document(doc.id).delete()
        deleted_count += 1
        if deleted_count % 100 == 0:
          logger.info(f'Deleted {deleted_count} documents.')
      
      logger.info(f'Deleted {deleted_count} documents.')
      return Response.success(f'Deleted {deleted_count} documents.')
    except ValueError as ve:
      logger.error(f"Value error in delete operation: {str(ve)}")
      return Response.error(f"Value error in delete operation: {str(ve)}")
    except Exception as e:
      logger.error(f"Error deleting documents from collection: {str(e)}")
      return Response.error(f"Error deleting documents from collection: {str(e)}")

  def update(self, path, data):
    logger.info(f'Updating document in collection: {path} with updates: {data}')
    try:
      if not path:
        raise ValueError("Path cannot be empty")
      if not data or not isinstance(data, dict):
        raise ValueError("Data must be a non-empty dictionary")
      
      self.db.document(path).update(data)
      logger.success(f'Document updated successfully.')
      return Response.success(f'Document updated successfully.')
    except ValueError as ve:
      logger.error(f"Value error in update operation: {str(ve)}")
      return Response.error(f"Value error in update operation: {str(ve)}")
    except Exception as e:
      logger.error(f"Error updating document: {str(e)}")
      return Response.error(f"Error updating document: {str(e)}")

  def create(self, data, path, id):
    logger.info(f'Adding document to collection: {path} with id: {id}')
    try:
      if not path:
        raise ValueError("Path cannot be empty")
      if not id:
        raise ValueError("ID cannot be empty")
      if not data or not isinstance(data, dict):
        raise ValueError("Data must be a non-empty dictionary")
      
      self.db.collection(path).document(id).set(data)
      logger.success(f'Document added successfully.')
      return Response.success(f'Document added successfully.')
    except ValueError as ve:
      logger.error(f"Value error in create operation: {str(ve)}")
      return Response.error(f"Value error in create operation: {str(ve)}")
    except Exception as e:
      logger.error(f"Error adding document: {str(e)}")
      return Response.error(f"Error adding document: {str(e)}")