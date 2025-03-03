from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from src.utils.logger import logger
from src.utils.response import Response
from src.utils.exception import handle_exception

import pandas as pd
from io import BytesIO, StringIO
import io

import os
import base64

from typing import Union

class GoogleDrive:
  # Class variable to store the singleton instance
  _instance = None
  
  @classmethod
  def get_instance(cls):
    """
    Get or create the singleton instance of GoogleDrive.
    
    Returns:
        GoogleDrive: The singleton instance of GoogleDrive
    """
    if cls._instance is None:
      cls._instance = cls()
    return cls._instance
  
  def __init__(self):
    # Skip initialization if instance already exists
    if GoogleDrive._instance is not None:
      logger.info('Using existing Drive instance')
      return
      
    logger.announcement('Initializing Drive', type='info')
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
      logger.announcement('Initialized Drive', type='success')
    except Exception as e:
      logger.error(f"Error initializing Drive: {str(e)}")

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
    try:
      fields = (
        'storageQuota,user,appInstalled,maxUploadSize,'
        'importFormats,exportFormats,canCreateDrives,'
        'folderColorPalette,driveThemes'
      )
      about = self.service.about().get(fields=fields).execute()
      logger.success('Successfully retrieved user information')
      return Response.success(about)
    except Exception as e:
      logger.error(f"Error retrieving user information: {str(e)}")
      return Response.error(f"Error retrieving user information: {str(e)}")

  def get_shared_drive_info(self, drive_name):
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

  def get_folder_info(self, parent_id, folder_name):
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

  def reset_folder(self, folder_id):
      response = self.get_files_in_folder(folder_id)
      if response['status'] == 'error':
          return Response.error(f'Error fetching files in folder.')
      files = response['content']
      if len(files) > 0:
          for f in files:
              response = self.delete_file(f['id'])
              if response['status'] == 'error':
                  return Response.error(f'Error deleting file.')
      return Response.success('Folder reset.')

  def get_files_in_folder(self, parent_id):
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

  def create_folder(self, folderName, parentFolderId):

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

  def get_file_info(self, parent_id, file_name):
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

  def get_file_info_by_id(self, file_id):
    logger.info(f'Getting file info for file: {file_id}')
    try:
      file = self.service.files().get(fileId=file_id, fields='id, name, parents, mimeType, size, modifiedTime, createdTime', supportsAllDrives=True).execute()
      logger.success(f"File found with ID: {file_id}")
      return Response.success(file)
    except Exception as e:
      logger.error(f"Error retrieving file info: {str(e)}")
      return Response.error(f"Error retrieving file info: {str(e)}")

  def rename_file(self, file_id, new_name):
    try:

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
      return Response.success(renamedFile)
    except Exception as e:
      logger.error(f"Error renaming file: {str(e)}")
      return Response.error(f"Error renaming file: {str(e)}")

  def move_file(self, f, newParentId):
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
  
  def upload_file(self, file_name: str, mime_type: str, f: Union[str, list], parent_folder_id: str) -> dict:
    logger.info(f"Uploading file: {file_name} to folder: {parent_folder_id}")
    file_metadata = {'name': file_name, 'mimeType': mime_type}

    if parent_folder_id is not None:
        file_metadata['parents'] = [parent_folder_id]

    try:
        # Handle base64 encoded data from React
        if isinstance(f, str):
            # Remove data URL prefix if present (e.g., "data:application/pdf;base64,")
            if ',' in f:
                f = f.split(',', 1)[1]
            file_bytes = base64.b64decode(f)
            # Configure MediaIoBaseUpload for better handling of large files
            media = MediaIoBaseUpload(
                BytesIO(file_bytes),
                mimetype=mime_type,
                resumable=True,  # Enable resumable uploads
                chunksize=1024*1024  # 1MB chunks
            )
        elif isinstance(f, list):
            df = pd.DataFrame(f)
            io_buffer = BytesIO()
            if mime_type == 'text/csv':
              df.to_csv(io_buffer, index=False)
            file_bytes = io_buffer.getvalue()
            media = MediaIoBaseUpload(
                BytesIO(file_bytes),
                mimetype=mime_type,
                resumable=True,
                chunksize=1024*1024
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
          )).execute()

        logger.success(f"Successfully uploaded file: {file_name} to folder: {parent_folder_id}")
        return Response.success(created_file)
    
    except Exception as e:
        logger.error(f"Error uploading file: {file_name}. Error: {str(e)}")
        return Response.error(f'Error uploading file: {str(e)}')
      
  def delete_file(self, file_id):

      logger.info(f"Deleting file with ID: {file_id}")

      try:
          deletedFile = self.service.files().delete(
            fileId=file_id, 
            supportsAllDrives=True, 
          ).execute()
          logger.success(f"Successfully deleted file with ID: {file_id}")
          return Response.success(deletedFile)
      except Exception as e:
          logger.error(f"Error deleting file with ID: {file_id}. Error: {str(e)}")
          return Response.error({'content': f'Error deleting file: {str(e)}', 'file_id': file_id})

  def download_file(self, file_id, parse=False):

    logger.info(f"Downloading file with ID: {file_id}")

    try:
        request = self.service.files().get_media(fileId=file_id)

        file_info = self.get_file_info_by_id(file_id)
        if file_info['status'] == 'error':
          return Response.error(file_info['content'])
        mime_type = file_info['content']['mimeType']

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
      elif mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        list_data = pd.read_excel(BytesIO(downloaded_file.getvalue())).fillna('').to_dict(orient='records')
      else:
        raise Exception("Unsupported MIME type for parsing.")
      logger.success("Successfully exported parsed file.")
      return list_data
    
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
        logger.error(f"An error occurred: {error}")
        return Response.error(error)
    
    except Exception as e:
        logger.error(f"Error exporting file: {str(e)}")
        return Response.error(f'Error exporting file: {str(e)}')
    
    logger.success("Successfully exported file.")
    if not parse:
      return Response.success(exported_file.getvalue())
    else:
      logger.warning("Exporting parsed file. This may take a while.")
      if mime_type == 'text/csv':
        list_data = pd.read_csv(StringIO(exported_file.getvalue().decode('latin1'))).fillna('').to_dict(orient='records')
        logger.success("Successfully exported parsed file.")
        return Response.success(list_data)
      elif mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        list_data = pd.read_excel(BytesIO(exported_file.getvalue())).fillna('').to_dict(orient='records')
        logger.success("Successfully exported parsed file.")
        return Response.success(list_data)
      else:
        logger.error("Unsupported MIME type for parsing.")
        return Response.error("Unsupported MIME type for parsing.")