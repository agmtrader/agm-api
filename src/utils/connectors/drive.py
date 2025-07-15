from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from src.utils.logger import logger
from src.utils.exception import handle_exception
from src.utils.managers.secret_manager import get_secret

import pandas as pd
from io import BytesIO, StringIO
import io
import json
import base64

from typing import Union

class GoogleDrive:
  _instance = None

  def __new__(cls):
    if cls._instance is None:
      cls._instance = super(GoogleDrive, cls).__new__(cls)
      cls._instance._initialized = False
    return cls._instance
  
  def __init__(self):
    if self._initialized:
      return
      
    logger.announcement('Initializing Drive', type='info')

    admin_creds = get_secret('OAUTH_PYTHON_CREDENTIALS_ADMIN')
    try:
      SCOPES = ["https://www.googleapis.com/auth/drive"]
      creds = Credentials(
        token=admin_creds['token'],
        refresh_token=admin_creds['refresh_token'],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=admin_creds['client_id'],
        client_secret=admin_creds['client_secret'],
        scopes=SCOPES
      )
      self.service = build('drive', 'v3', credentials=creds)
      logger.announcement('Initialized Drive', type='success')
    except Exception as e:
      raise Exception(e)
    self._initialized = True

  @handle_exception
  def get_user_info(self):
    """
    Gets information about the user, including storage quota and capabilities.
    
    Returns:
        dict: A Response object containing:
            - On success: {'status': 'success', 'content': user_info}
                where user_info includes storage quota, user details, and other Drive capabilities
            - On failure: {'status': 'error', 'content': error_message}
    """
    logger.info('Getting user information from Drive')
    fields = (
      'storageQuota,user,appInstalled,maxUploadSize,'
      'importFormats,exportFormats,canCreateDrives,'
      'folderColorPalette,driveThemes'
    )
    about = self.service.about().get(fields=fields).execute()
    logger.success('Successfully retrieved user information')
    return about

  @handle_exception
  def get_shared_drive_info(self, drive_name):
    logger.info(f'Getting shared drive info for drive: {drive_name}')
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
      raise Exception(f"No shared drive found with name '{drive_name}'")
    
    logger.success(f"Shared drive found with name '{drive_name}'")
    return shared_drives[0]

  @handle_exception
  def get_folder_info(self, parent_id, folder_name):
    logger.info(f'Getting folder info for folder: {folder_name} in parent: {parent_id}')
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
      raise Exception(f"No folder found with name '{folder_name}' in parent '{parent_id}'")
    
    logger.success(f"Folder found with name '{folder_name}' in parent '{parent_id}'")
    return folders[0]

  @handle_exception
  def create_folder(self, folderName, parentFolderId):
    logger.info(f"Creating folder: {folderName} in folder: {parentFolderId}")

    fileMetadata = {
        'name': folderName,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    if parentFolderId is not None:
        fileMetadata['parents'] = [parentFolderId]
    else:
        raise Exception('No parent folder ID provided.')
    
    folder = self.service.files().create(body=fileMetadata, fields='id, name, parents, mimeType, size, modifiedTime, createdTime').execute()
    logger.success(f"Successfully created folder: {folderName} in folder: {parentFolderId}")
    return folder

  @handle_exception
  def clear_folder(self, folder_id):
      try:
        files = self.get_files_in_folder(folder_id)
      except Exception as e:
        raise Exception(f'Error fetching files in folder: {folder_id}')

      if len(files) > 0:
          for f in files:
              self.delete_file(f['id'])
              
      logger.success('Folder reset.')
      return {'status': 'success'}

  @handle_exception
  def get_files_in_folder(self, parent_id):
    logger.info(f'Getting files in folder: {parent_id}')
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
    return files

  @handle_exception
  def get_file_info(self, parent_id, file_name):
    logger.info(f'Getting file info for file: {file_name} in parent: {parent_id}')
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
      raise Exception(f"No file found with name '{file_name}' in parent '{parent_id}'")
    logger.success(f"File found with name '{file_name}' in parent '{parent_id}'")
    return files[0]

  @handle_exception
  def get_file_info_by_id(self, file_id):
    logger.info(f'Getting file info for file: {file_id}')
    f = self.service.files().get(fileId=file_id, fields='id, name, parents, mimeType, size, modifiedTime, createdTime', supportsAllDrives=True).execute()
    logger.success(f"File found with ID: {file_id}")
    return f

  @handle_exception
  def rename_file(self, file_id, new_name):
    logger.info(f'Renaming file {file_id} to {new_name}')
    file_metadata = {
      'name': new_name
    }

    renamedFile = (
      self.service.files().update(
        fileId=file_id,
        body=file_metadata,
        supportsAllDrives=True,
        fields='id, name, parents, mimeType, size, modifiedTime, createdTime'
      )).execute()

    logger.success(f'Successfully renamed file {file_id} to {new_name}')
    return renamedFile
  
  @handle_exception
  def move_file(self, f, newParentId):
    logger.info(f'Moving file: {f} to new parent: {newParentId}')

    moved_file = self.service.files().update(
        fileId=f['id'],
        removeParents=f['parents'][0],
        addParents=newParentId,
        fields='id, parents, name, mimeType, size, modifiedTime, createdTime',
        supportsAllDrives=True,
    ).execute()

    logger.success(f'Successfully moved file: {f["name"]}')
    return moved_file

  def upload_file(self, file_name: str, mime_type: str, file_data: Union[str, list[dict]], parent_folder_id: str) -> dict:
    """
    Upload a file to Google Drive. Supports two types of uploads:
    1. Base64 encoded file data from web/React applications
    2. Array of dictionaries to be converted to CSV/Excel

    Args:
        file_name (str): Name of the file to create
        mime_type (str): MIME type of the file
        file_data (Union[str, list[dict]]): Either base64 encoded string or array of dictionaries
        parent_folder_id (str): ID of the parent folder in Drive

    Returns:
        dict: Created file metadata from Drive
    """
    logger.info(f"Uploading file: {file_name} to folder: {parent_folder_id}")

    # Handle array of dictionaries (convert to CSV/Excel)
    if isinstance(file_data, list):
      df = pd.DataFrame(file_data)
      buffer = BytesIO()
      
      if mime_type == 'text/csv':
        df.to_csv(buffer, index=False)
      elif mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        df.to_excel(buffer, index=False)
      else:
        raise ValueError(f"Unsupported MIME type {mime_type} for dictionary array upload")
      
      file_bytes = buffer.getvalue()

    # Handle base64 encoded string
    else:

      if ',' in file_data:  # Remove data URL prefix if present
        file_data = file_data.split(',', 1)[1]
      file_bytes = base64.b64decode(file_data)

    # Configure upload with chunking for better performance
    media = MediaIoBaseUpload(
      BytesIO(file_bytes),
      mimetype=mime_type,
      resumable=True,
      chunksize=1024*1024  # 1MB chunks
    )

    file_metadata = {
      'name': file_name,
      'parents': [parent_folder_id],
      'mimeType': mime_type
    }

    created_file = (
      self.service.files().create(
        supportsAllDrives=True,
        body=file_metadata,
        media_body=media,
        fields='id, name, parents, mimeType, size, modifiedTime, createdTime'
      )
    ).execute()

    logger.success(f"Successfully uploaded file: {file_name} to folder: {parent_folder_id}")
    return created_file
  
  @handle_exception
  def delete_file(self, file_id):

      logger.info(f"Deleting file with ID: {file_id}")
      deletedFile = self.service.files().delete(
        fileId=file_id, 
        supportsAllDrives=True, 
      ).execute()
      logger.success(f"Successfully deleted file with ID: {file_id}")
      return deletedFile

  @handle_exception
  def download_file(self, file_id, parse=False):

    logger.info(f"Downloading file with ID: {file_id}")

    try:
        request = self.service.files().get_media(fileId=file_id)

        file_info = self.get_file_info_by_id(file_id)
        
        mime_type = file_info['mimeType']

        downloaded_file = io.BytesIO()
        downloader = MediaIoBaseDownload(downloaded_file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logger.info(f"Download {int(status.progress() * 100)}.")

    except HttpError as e:
        raise Exception(e)
    
    except Exception as e:
       raise Exception(e)
        
    logger.success("Successfully downloaded file.")
    
    if not parse:
      return downloaded_file.getvalue()
    else:
      logger.warning("Exporting parsed file. This may take a while.")
      if mime_type == 'text/csv':
        list_data = pd.read_csv(StringIO(downloaded_file.getvalue().decode('latin1'))).fillna('').to_dict(orient='records')
      elif mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mime_type == 'application/vnd.ms-excel':
        list_data = pd.read_excel(BytesIO(downloaded_file.getvalue())).fillna('').to_dict(orient='records')
      else:
        raise Exception("Unsupported MIME type for parsing.")
      
      logger.success("Successfully exported parsed file.")
      return list_data

  @handle_exception
  def export_file(self, file_id, mime_type, parse=False):
    logger.info(f"Exporting file with ID: {file_id} to MIME type: {mime_type}")

    try:
        request = self.service.files().export_media(
            fileId=file_id,
            mimeType=mime_type,
        )
        exported_file = io.BytesIO()
        downloader = MediaIoBaseDownload(exported_file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logger.info(f"Export {int(status.progress() * 100)}%.")

    except HttpError as error:
        raise Exception(error)
    
    except Exception as e:
        raise Exception(e)
    
    logger.success("Successfully exported file.")
    if not parse:
      return exported_file.getvalue()
    else:
      logger.warning("Exporting parsed file. This may take a while.")
      if mime_type == 'text/csv':
        list_data = pd.read_csv(StringIO(exported_file.getvalue().decode('latin1'))).fillna('').to_dict(orient='records')
        logger.success("Successfully exported parsed file.")
        return list_data
      elif mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        list_data = pd.read_excel(BytesIO(exported_file.getvalue())).fillna('').to_dict(orient='records')
        logger.success("Successfully exported parsed file.")
        return list_data
      else:
        raise Exception("Unsupported MIME type for parsing.")