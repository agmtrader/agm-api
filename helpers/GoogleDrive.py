from googleapiclient.discovery import build

from google.oauth2.credentials import Credentials

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