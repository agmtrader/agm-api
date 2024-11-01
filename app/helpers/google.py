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
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from jinja2 import Environment, FileSystemLoader
from premailer import transform

from io import BytesIO

class GoogleDrive:
  
  def __init__(self):
    logger.announcement('Initializing GoogleDrive connection.', type='info')
    try:
      SCOPES = ["https://www.googleapis.com/auth/drive"]
      creds = Credentials(
        token=os.getenv('ADMIN_TOKEN'),
        refresh_token=os.getenv('ADMIN_REFRESH_TOKEN'),
        token_uri=os.getenv('ADMIN_TOKEN_URI'),
        client_id=os.getenv('ADMIN_CLIENT_ID'),
        client_secret=os.getenv('ADMIN_CLIENT_SECRET'),
        scopes=SCOPES
      )
      self.service = build('drive', 'v3', credentials=creds)
      logger.announcement('Initialized GoogleDrive connection.', type='success')
    except Exception as e:
      logger.error(f"Error initializing GoogleDrive: {str(e)}")

  def getSharedDriveInfo(self, drive_name):
    logger.info(f'Getting shared drive info for drive: {drive_name}')
    try:
      shared_drives = []
      page_token = None
      while True:
        response = (
          self.service.drives()
          .list(
              q=f"name = '{drive_name}'",
              fields="nextPageToken, drives(id, name)",
              supportsAllDrives=True,
              includeItemsFromAllDrives=True,
              pageToken=page_token
          ).execute())
        shared_drives.extend(response.get('drives', []))
        page_token = response.get('nextPageToken')
        if not page_token:
          break

      if not shared_drives:
        logger.error(f"No shared drive found with name '{drive_name}'")
        return Response.error(f"No shared drive found with name '{drive_name}'")
      logger.success(f"Shared drive found with name '{drive_name}'")
      return Response.success(shared_drives[0])
    except Exception as e:
      logger.error(f"Error retrieving shared drive info: {str(e)}")
      return Response.error(f"Error retrieving shared drive info: {str(e)}")

  def getFolderInfo(self, parent_id, folder_name):
    logger.info(f'Getting folder info for folder: {folder_name} in parent: {parent_id}')
    try:
      folders = []
      page_token = None
      while True:
        response = (
            self.service.files()
            .list(
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                q=f"name = '{folder_name}' and '{parent_id}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, parents)",
                pageToken=page_token
            ).execute())
        folders.extend(response.get('files', []))
        page_token = response.get('nextPageToken')
        if not page_token:
          break

      if not folders:
        logger.error(f"No folder found with name '{folder_name}' in parent '{parent_id}'")
        return Response.error(f"No folder found with name '{folder_name}' in parent '{parent_id}'")
      logger.success(f"Folder found with name '{folder_name}' in parent '{parent_id}'")
      return Response.success(folders[0])
    except Exception as e:
      logger.error(f"Error retrieving folder info: {str(e)}")
      return Response.error(f"Error retrieving folder info: {str(e)}")

  def resetFolder(self, folder_id):
      response = self.getFilesInFolder(folder_id)
      if response['status'] == 'error':
          return Response.error(f'Error fetching files in folder.')
      files = response['content']
      if len(files) > 0:
          for f in files:
              response = self.deleteFile(f['id'])
              if response['status'] == 'error':
                  return Response.error(f'Error deleting file.')
      return Response.success('Folder reset.')

  def getFilesInFolder(self, parent_id):
    logger.info(f'Getting files in folder: {parent_id}')
    try:
      files = []
      page_token = None
      while True:
        response = (
            self.service.files().list(
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                q=f"'{parent_id}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, parents, mimeType, size, modifiedTime, createdTime)",
                pageToken=page_token
            ).execute())
        files.extend(response.get('files', []))
        page_token = response.get('nextPageToken')
        if not page_token:
          break
      
      logger.success(f'{len(files)} files found in folder: {parent_id}')
      return Response.success(files)
    except Exception as e:
      logger.error(f"Error retrieving files in folder: {str(e)}")
      return Response.error(f"Error retrieving files in folder: {str(e)}")

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

  def getFileInfo(self, parent_id, file_name):
    logger.info(f'Getting file info for file: {file_name} in parent: {parent_id}')
    try:
      files = []
      page_token = None
      while True:
        response = (
            self.service.files()
            .list(
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                q=f"name = '{file_name}' and '{parent_id}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, parents)",
                pageToken=page_token
            ).execute())
        files.extend(response.get('files', []))
        page_token = response.get('nextPageToken')
        if not page_token:
          break

      if not files:
        logger.error(f"No file found with name '{file_name}' in parent '{parent_id}'")
        return Response.error(f"No file found with name '{file_name}' in parent '{parent_id}'")
      logger.success(f"File found with name '{file_name}' in parent '{parent_id}'")
      return Response.success(files[0])
    except Exception as e:
      logger.error(f"Error retrieving file info: {str(e)}")
      return Response.error(f"Error retrieving file info: {str(e)}")



  def renameFile(self, fileId, newName):
    try:

      logger.info(f'Renaming file {fileId} to {newName}')
      file_metadata = {
        'name': newName
      }

      renamedFile = (
        self.service.files().update(
          fileId=fileId,
          body=file_metadata,
          supportsAllDrives=True,
          fields='id, name, parents, mimeType, size, modifiedTime, createdTime'
        )).execute()

      logger.success(f'Successfully renamed file {fileId} to {newName}')
      return Response.success(renamedFile)
    except Exception as e:
      logger.error(f"Error renaming file: {str(e)}")
      return Response.error(f"Error renaming file: {str(e)}")

  def moveFile(self, f, newParentId):
    logger.info(f'Moving file: {f} to new parent: {newParentId}')
    try:
      
      moved_file = self.service.files().update(
          fileId=f['id'],
          removeParents=f['parents'][0],
          addParents=newParentId,
          fields='id, parents, name, mimeType, size, modifiedTime, createdTime',
          supportsAllDrives=True,
      ).execute()

      logger.success(f'Successfully moved file: {f["name"]}')
      return Response.success(moved_file)
    except Exception as e:
      logger.error(f"Error moving file: {str(e)}")
      return Response.error(f"Error moving file: {str(e)}")
  
  def uploadFile(self, fileName, mimeType, f, parentFolderId):
    logger.info(f"Uploading file: {fileName} to folder: {parentFolderId}")
    fileMetadata = {'name': fileName, 'mimeType': mimeType}

    if parentFolderId is not None:
        fileMetadata['parents'] = [parentFolderId]
    else:
        logger.error("No parent folder ID provided.")
        return Response.error('No parent folder ID provided.')
    
    try:
        # Handle base64 encoded data from React
        if isinstance(f, str) and f.startswith('data:'):
            # Extract the base64 encoded data
            header, encoded = f.split(",", 1)
            file_bytes = base64.b64decode(encoded)
            media = MediaIoBaseUpload(BytesIO(file_bytes), mimetype=mimeType)
        # Handle other file types (keeping existing logic)
        elif isinstance(f, io.IOBase):
            media = MediaIoBaseUpload(f, mimetype=mimeType)
        elif isinstance(f, bytes):
            media = MediaIoBaseUpload(BytesIO(f), mimetype=mimeType)
        elif isinstance(f, pd.DataFrame):
            csv_buffer = BytesIO()
            f.to_csv(csv_buffer, index=False)
            csv_bytes = csv_buffer.getvalue()
            media = MediaIoBaseUpload(BytesIO(csv_bytes), mimetype='text/csv')
        elif isinstance(f, list):
            df = pd.DataFrame(f)
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_bytes = csv_buffer.getvalue()
            media = MediaIoBaseUpload(BytesIO(csv_bytes), mimetype='text/csv')
        else:
            raise Exception('Unsupported file type')

        file_metadata = {
            'name': fileName,
            'parents': [parentFolderId],
            'mimeType': mimeType
        }

        created_file = (
            self.service.files().create(
            supportsAllDrives=True,
            body=file_metadata,
            media_body=media,
            fields='id, name, parents, mimeType, size, modifiedTime, createdTime'
          )).execute()

        logger.success(f"Successfully uploaded file: {fileName} to folder: {parentFolderId}")
        return Response.success(created_file)
    
    except Exception as e:
        logger.error(f"Error uploading file: {fileName}. Error: {str(e)}")
        return Response.error(f'Error uploading file: {str(e)}')
          
  def deleteFile(self, fileId):

      logger.info(f"Deleting file with ID: {fileId}")

      try:
          deletedFile = self.service.files().delete(
            fileId=fileId, 
            supportsAllDrives=True, 
          ).execute()
          logger.success(f"Successfully deleted file with ID: {fileId}")
          return Response.success(deletedFile)
      except Exception as e:
          logger.error(f"Error deleting file with ID: {fileId}. Error: {str(e)}")
          return Response.error({'content': f'Error deleting file: {str(e)}', 'file_id': fileId})

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

  def exportFile(self, fileId, mimeType):
    logger.info(f"Exporting file with ID: {fileId} to MIME type: {mimeType}")

    try:
        request = self.service.files().export_media(
            fileId=fileId,
            mimeType=mimeType
        )
        exported_file = io.BytesIO()
        downloader = MediaIoBaseDownload(exported_file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logger.info(f"Export {int(status.progress() * 100)}%.")

        logger.success("Successfully exported file.")
        return Response.success(exported_file.getvalue())

    except HttpError as error:
        logger.error(f"An error occurred: {error}")
        return Response.error(error)
    
    except Exception as e:
        logger.error(f"Error exporting file: {str(e)}")
        return Response.error(f'Error exporting file: {str(e)}')

class Gmail:

  def __init__(self):
    logger.announcement('Initializing Gmail connection.', type='info')
    SCOPES = ["https://mail.google.com/"]
    try:
      creds = Credentials(
        token=os.getenv('INFO_TOKEN'),
        refresh_token=os.getenv('INFO_REFRESH_TOKEN'),
        token_uri=os.getenv('INFO_TOKEN_URI'),
        client_id=os.getenv('INFO_CLIENT_ID'),
        client_secret=os.getenv('INFO_CLIENT_SECRET'),
        scopes=SCOPES
      )
      self.service = build("gmail", "v1", credentials=creds)
      logger.announcement('Initialized Gmail connection.', type='success')
    except Exception as e:
      logger.error(f"Error initializing Gmail: {str(e)}")

  def create_html_email(self, plain_text, subject):
    logger.info(f'Creating HTML email with subject: {subject}')

    try:
        # Load the HTML template
        env = Environment(loader=FileSystemLoader('app/helpers/email_templates'))
        template = env.get_template('trade_ticket.html')

        # Render the template with the plain text content
        html_content = template.render(content=plain_text, subject=subject)

        # Inline the CSS
        html_content_inlined = transform(html_content)
        logger.info(f'HTML content inlined: {html_content_inlined}')

        # Create a multipart message
        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = "info@agmtechnology.com"

        # Attach plain text and HTML versions
        text_part = MIMEText(plain_text, 'plain')
        html_part = MIMEText(html_content_inlined, 'html')
        
        message.attach(text_part)
        message.attach(html_part)

        # Create the final multipart/related message
        final_message = MIMEMultipart('related')
        final_message['Subject'] = subject
        final_message['From'] = "info@agmtechnology.com"
        final_message.attach(message)

        # Attach the logo image
        logo_path = 'app/assets/agm-logo.png'
        with open(logo_path, 'rb') as logo_file:
            logo_mime = MIMEImage(logo_file.read())
            logo_mime.add_header('Content-ID', '<logo>')
            final_message.attach(logo_mime)

        logger.success(f'Successfully created HTML email with subject: {subject}')
        return Response.success(final_message)
    
    except Exception as e:
        logger.error(f"Error creating HTML email: {str(e)}")
        return Response.error(f"Error creating HTML email: {str(e)}")

  def sendClientEmail(self, plain_text, client_email, subject):
    try:
        logger.info(f'Sending client email to: {client_email}')
        response = self.create_html_email(plain_text, subject)

        message = response['content']
        message['To'] = client_email
        message['Bcc'] = "cr@agmtechnology.com,aa@agmtechnology.com,jc@agmtechnology.com,hc@agmtechnology.com,rc@agmtechnology.com"

        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = {"raw": raw_message}

        send_message = (
            self.service.users()
            .messages()
            .send(userId="me", body=create_message)
            .execute()
        )
        logger.success(f'Successfully sent client email to: {client_email}')
        return Response.success({'emailId': send_message["id"]})
    except Exception as e:
        return Response.error(f"Error sending client email: {str(e)}")

class Firebase:

  def __init__(self):
    logger.announcement('Initializing Firebase connection.', type='info')
    try:
      cred = credentials.Certificate({
          "type": os.getenv('FIREBASE_TYPE'),
          "project_id": os.getenv('FIREBASE_PROJECT_ID'),
          "private_key_id": os.getenv('FIREBASE_PRIVATE_KEY_ID'),
          "private_key": os.getenv('FIREBASE_PRIVATE_KEY').replace('"', '').replace('\\n', '\n'),
          "client_email": os.getenv('FIREBASE_CLIENT_EMAIL'),
          "client_id": os.getenv('FIREBASE_CLIENT_ID'),
          "auth_uri": os.getenv('FIREBASE_AUTH_URI'),
          "token_uri": os.getenv('FIREBASE_TOKEN_URI'),
          "auth_provider_x509_cert_url": os.getenv('FIREBASE_AUTH_PROVIDER_X509_CERT_URL'),
          "client_x509_cert_url": os.getenv('FIREBASE_CLIENT_X509_CERT_URL'),
          "universe_domain": os.getenv('FIREBASE_UNIVERSE_DOMAIN')
      })
      firebase_admin.initialize_app(cred)
      self.db = firestore.client()
      logger.announcement('Initialized Firebase connection.', type='success')
    except ValueError:
      try:
        self.db = firestore.client()
      except Exception as e:
        logger.error(f"Error initializing Firebase: {str(e)}")
    except Exception as e:
      logger.error(f"Error initializing Firebase: {str(e)}")

  # listing subcollections basically lists all the csv files in a folder
  def listSubcollections(self, path):
    logger.info(f'Listing subcollections in document: {path}')
    try:
        if not path:
            raise ValueError("Path cannot be empty")
        
        # Get a reference to the document
        doc_ref = self.db.document(path)
        
        # List the subcollections of the document
        collections = doc_ref.collections()
        
        results = []
        for collection in collections:
            # For each subcollection, get its documents
            docs = collection.stream()
            subcollection_data = []
            for doc in docs:
                doc_dict = doc.to_dict()
                doc_dict['id'] = doc.id
                subcollection_data.append(doc_dict)
            
            results.append({
                'collection_id': collection.id,
                'documents': subcollection_data
            })
        
        logger.success(f'Successfully listed subcollections.')
        return Response.success(results)
    except Exception as e:
        return Response.error(f"Error listing subcollections: {str(e)}")

  # collections are basically csv documents
  def clear_collection(self, path):
    logger.info(f'Clearing collection: {path}')
    try:
      docs = self.db.collection(path).list_documents()
      for i, doc in enumerate(docs):
        doc.delete()
        if i != 0:
          if i % 10 == 0:
            logger.info(f'Deleted {i} documents.')
          elif i % 100 == 0:
            logger.announcement(f'Deleted {i} documents.', type='info')
      logger.success(f'Collection cleared successfully.')
      return Response.success(f'Collection cleared successfully.')
    except Exception as e: 
      logger.error(f"Error clearing collection: {str(e)}")
      return Response.error(f"Error clearing collection: {str(e)}")
  
  # upload collection is used to upload a csv file or pandas DataFrame to a folder
  def upload_collection(self, path, data):
    try:
      # Clear the collection before uploading
      self.clear_collection(path)
      
      logger.info(f'Uploading collection: {path}')
      # Convert pandas DataFrame to list of dictionaries if necessary
      if isinstance(data, pd.DataFrame):
        data = data.to_dict('records')
      
      # Iterate through the data and add each row as a document]
      for i, row in enumerate(data):
        self.db.collection(path).add(row)
        if i != 0:
          if i % 10 == 0:
            logger.info(f'Uploaded {i} documents.')
          elif i % 100 == 0:
            logger.announcement(f'Uploaded {i} documents.', type='info')
          
      logger.success(f'Collection uploaded successfully.')
      return Response.success(f'Collection uploaded successfully.')
    
    except Exception as e:
      logger.error(f"Error uploading collection: {str(e)}")
      return Response.error(f"Error uploading collection: {str(e)}")
  

  # crud actions on collections (query rows)
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
  
  def delete(self, path, query=None):
    logger.info(f'Deleting documents in collection: {path} with query: {query}')
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
      
      batch = self.db.batch()
      docs = ref.stream()
      deleted_count = 0
      for doc in docs:
        batch.delete(doc.reference)
        deleted_count += 1
        if deleted_count % 100 == 0:
          logger.info(f'Deleting {deleted_count} documents.')
      
      batch.commit()
      logger.success(f'Deleted {deleted_count} documents.')
      return Response.success(f'Deleted {deleted_count} documents.')
    except ValueError as ve:
      logger.error(f"Value error in delete operation: {str(ve)}")
      return Response.error(f"Value error in delete operation: {str(ve)}")
    except TypeError as te:
      logger.error(f"Type error in delete operation: {str(te)}")
      return Response.error(f"Type error in delete operation: {str(te)}")
    except Exception as e:
      logger.error(f"Error deleting documents from collection: {str(e)}")
      return Response.error(f"Error deleting documents from collection: {str(e)}")

  def update(self, path, data, query=None):
    logger.info(f'Updating documents in collection: {path} with updates: {data} and query: {query}')
    try:
      if not path:
        raise ValueError("Path cannot be empty")
      if not data or not isinstance(data, dict):
        raise ValueError("Data must be a non-empty dictionary")
      
      ref = self.db.collection(path)
      if query:
        if not isinstance(query, dict):
          raise TypeError("Query must be a dictionary")
        for key, value in query.items():
          if not isinstance(key, str):
            raise TypeError("Query keys must be strings")
          ref = ref.where(filter=firestore.FieldFilter(key, "==", value))
      
      batch = self.db.batch()
      docs = ref.stream()
      updated_count = 0
      for doc in docs:
        batch.update(doc.reference, data)
        updated_count += 1
      
      batch.commit()
      logger.success(f'{updated_count} documents updated successfully.')
      return Response.success(f'{updated_count} documents updated successfully.')
    except ValueError as ve:
      logger.error(f"Value error in update operation: {str(ve)}")
      return Response.error(f"Value error in update operation: {str(ve)}")
    except TypeError as te:
      logger.error(f"Type error in update operation: {str(te)}")
      return Response.error(f"Type error in update operation: {str(te)}")
    except Exception as e:
      logger.error(f"Error updating documents: {str(e)}")
      return Response.error(f"Error updating documents: {str(e)}")

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



