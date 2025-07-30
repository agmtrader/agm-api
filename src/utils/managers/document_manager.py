from src.utils.exception import handle_exception
from src.utils.connectors.drive import GoogleDrive
from src.utils.connectors.supabase import db

Drive = GoogleDrive()


class DocumentManager:
    @handle_exception
    def upload_document(account_id, file_name, file_length, sha1_checksum, mime_type, data):
        """
        Uploads a document to Google Drive and saves metadata/content in the database.
        Args:
            account_id (str): UUID of the account to link the document to.
            document_data (dict): Data matching ibkr_document_schema.
            drive_folder_id (str): Google Drive folder ID to upload to.
        Returns:
            dict: Information about the uploaded document.
        """

        # Upload to Google Drive
        Drive.upload_file(
            file_name=file_name,
            mime_type=mime_type,
            file_data=data,
            parent_folder_id='1111'
        )

        # Save to document table
        document_id = db.create(
            table='document',
            data={
                'file_name': file_name,
                'file_length': file_length,
                'sha1_checksum': sha1_checksum,
                'mime_type': mime_type,
                'data': data
            }
        )

        # Link to account_document
        account_document_id = db.create(
            table='account_document',
            data={
                'account_id': account_id,
                'document_id': document_id
            }
        )

        return account_document_id
