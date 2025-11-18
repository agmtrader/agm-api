from src.utils.exception import handle_exception
from src.utils.connectors.drive import GoogleDrive
from src.utils.connectors.supabase import db

Drive = GoogleDrive()

class DocumentManager:
    @handle_exception
    def upload_document(self, account_id, file_name, file_length, sha1_checksum, mime_type, data, category, type, issued_date, expiry_date):
        """
        Uploads a document to the database and links it to an account.
        Args:
            account_id (str): UUID of the account to link the document to.
            file_name (str): Name of the file.
            file_length (int): Length of the file.
            sha1_checksum (str): SHA1 checksum of the file.
            mime_type (str): MIME type of the file.
            data (str): Base64 encoded data of the file.
        Returns:
            dict: Information about the uploaded document.
        """

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
                'document_id': document_id,
                'category': category,
                'type': type,
                'issued_date': issued_date,
                'expiry_date': expiry_date
            }
        )

        return account_document_id
