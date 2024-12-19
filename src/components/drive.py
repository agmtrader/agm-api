from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from src.utils.logger import logger
from src.utils.response import Response

import pandas as pd
from io import BytesIO
import io

import os
import base64

from typing import Union

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

  def rename_file(self, fileId, newName):
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
  
  def upload_file(self, fileName: str, mimeType: str, f: Union[str, io.IOBase, list], parentFolderId: str) -> dict:
    """
    Uploads a file to Google Drive in a specified folder.

    Args:
        fileName (str): The name to give the uploaded file in Google Drive
        mimeType (str): The MIME type of the file being uploaded
        f (Union[str, io.IOBase, list]): The file content to upload. Can be:
            - base64 encoded string (from third parties)
            - file object (io.IOBase)
            - list (will be converted to CSV via pandas DataFrame)
        parentFolderId (str): The ID of the folder where the file should be uploaded

    Returns:
        dict: A Response object containing:
            - On success: {'status': 'success', 'content': file_metadata}
                where file_metadata includes id, name, parents, mimeType, size, modifiedTime, createdTime
            - On failure: {'status': 'error', 'content': error_message}

    Raises:
        Exception: If an unsupported file type is provided or if upload fails

    """
    logger.info(f"Uploading file: {fileName} to folder: {parentFolderId}")
    fileMetadata = {'name': fileName, 'mimeType': mimeType}

    if parentFolderId is not None:
        fileMetadata['parents'] = [parentFolderId]

    try:
        # Handle base64 encoded data from React
        if isinstance(f, str):
            if f.startswith('data:'):
                header, encoded = f.split(",", 1)
                file_bytes = base64.b64decode(encoded)
            else:
                # Handle plain string content
                file_bytes = f.encode('utf-8')
            media = MediaIoBaseUpload(BytesIO(file_bytes), mimetype=mimeType)
        elif isinstance(f, io.IOBase):
            media = MediaIoBaseUpload(f, mimetype=mimeType)
        elif isinstance(f, list):
            df = pd.DataFrame(f)
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_bytes = csv_buffer.getvalue()
            media = MediaIoBaseUpload(BytesIO(csv_bytes), mimetype='text/csv')

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
      
  def delete_file(self, fileId):

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

  def download_file(self, fileId):

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
    
    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        return Response.error(f"Error downloading file: {str(e)}")
    
    logger.success("Successfully downloaded file.")
    return Response.success(downloaded_file.getvalue())

  def export_file(self, fileId, mimeType):
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