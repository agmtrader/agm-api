from src.utils.exception import handle_exception
from src.utils.connectors.drive import GoogleDrive
from src.utils.connectors.supabase import db

Drive = GoogleDrive()

    
class Bucket:
    """
    Represents a bucket in the Document Center
    """
    def __init__(self, id: str, name: str, drive_id: str, database_table: str):
        self.id = id
        self.name = name
        self.drive_id = drive_id
        self.database_table = database_table
        self.files = []
        
    def to_dict(self):
        return {
            'id': self.id,
            'drive_id': self.drive_id,
            'database_table': self.database_table,
            'name': self.name,
            'files': self.files
        }
    
class DocumentCenter:
    _instances = {}

    def __new__(cls, type=None):

        if type == 'clients':
            buckets = [
                Bucket(
                    id='address',
                    name='Proof of Address',
                    drive_id='1tuS0EOHoFm9TiJlv3uyXpbMrSgIKC2QL',
                    database_table='poa_document',
                ),
                Bucket(
                    id='identity',
                    name='Proof of Identity',
                    drive_id='1tuS0EOHoFm9TiJlv3uyXpbMrSgIKC2QL',
                    database_table='poi_document',
                )
            ]
        else:
            raise Exception('Type is required')
        
        bucket_key = str(buckets)
        if bucket_key not in cls._instances:
            cls._instances[bucket_key] = super(DocumentCenter, cls).__new__(cls)
            cls._instances[bucket_key]._initialized = False
            cls._instances[bucket_key].buckets = buckets
        return cls._instances[bucket_key]

    def __init__(self, type=None):
        if self._initialized:
            return
            
        self._initialized = True

    @handle_exception
    def read(self, query: dict = None):
        """
        Reads the entire Document Center
        """
        buckets = []

        for bucket in self.buckets:

            files_in_bucket = db.read(table=bucket.database_table, query=query)
            bucket_files = []

            for f in files_in_bucket:
                file_info = db.read(table='document', query={'id': f['document_id'], **query})
                if len(file_info) == 0:
                    raise Exception(f"File with ID {f['document_id']} not found")
                entire_file = {
                    **f,
                    **file_info[0]
                }
                bucket_files.append(entire_file)

            bucket.files = bucket_files

        buckets = [bucket.to_dict() for bucket in self.buckets]

        if len(buckets) == 0:
            raise Exception("No files found")
        
        return buckets
    
    @handle_exception
    def delete(self, document: dict, bucket_id: str) -> None:
        """
        Deletes a file from the Document Center

        Args:
            document (dict): Dictionary containing document information with keys:
                - DocumentID: str
                - DocumentInfo: dict
                - FileInfo: dict
                - UserID: str
            bucket_id (str): The ID of the bucket

        Raises:
            ValueError: If required document fields are missing
            Exception: If bucket is not found
        """

        # Find and validate bucket
        bucket = next((bucket for bucket in self.buckets if bucket.id == bucket_id), None)
        if not bucket:
            raise Exception(f"Bucket with ID {bucket_id} not found")
        
        # Delete from database and drive
        Drive.delete_file(document['drive_id'])
        db.delete(table=bucket.database_table, query={'id': document['id']})
        return
    
    @handle_exception
    def upload(self, f: dict, document_info: dict, user_id: str, bucket_id: str):
        """
        Uploads a file to the Document Center

        Args:
            f (dict): The file to upload
            document_info (dict): Operational information of the document
            user_id (str): The ID of the uploader
        """
        
        # Find and validate bucket
        bucket = next((bucket for bucket in self.buckets if bucket.id == bucket_id), None)
        if not bucket:
            raise Exception(f"Bucket with ID {bucket_id} not found")
        
        # Upload file to drive
        file_info = Drive.upload_file(file_name=f['file_name'], mime_type=f['mime_type'], file_data=f['file_data'], parent_folder_id=bucket.drive_id)

        document = {
            'drive_id': file_info['id'],
            'mime_type': file_info['mimeType'],
            'name': file_info['name'],
            'parents': file_info['parents'],
            'user_id': user_id
        }

        # Upload to database
        document_id = db.create(table='document', data=document)

        if bucket.id == 'address':
            unique_document = {
                'type':document_info['type'],
                'issued_date':document_info['issued_date'],
                'document_id': document_id,
                'account_id': document_info['account_id']
            }
        elif bucket.id == 'identity':
            unique_document = {
                'gender': document_info['gender'],
                'country_of_issue': document_info['country_of_issue'],
                'issued_date':document_info['issued_date'],
                'type': document_info['type'],
                'full_name': document_info['full_name'],
                'id_number': document_info['id_number'],
                'date_of_birth': document_info['date_of_birth'],
                'expiration_date': document_info['expiration_date'],
                'country_of_birth': document_info['country_of_birth'],
                'document_id': document_id,
                'account_id': document_info['account_id']
            }

        created_document_id = db.create(table=bucket.database_table, data=unique_document)
        return created_document_id