from googleapiclient.discovery import build

class GoogleDrive:
  
  def init(self):
    self.service = self.authenticateGoogleDrive()
    return self.service

  def authenticateGoogleDrive():
    # Find a shared drive's info
    service = build('drive', 'v3')
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