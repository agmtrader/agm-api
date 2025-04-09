from datetime import datetime
from src.lib.entities.document import Document

from src.utils.exception import handle_exception
from src.helpers.drive import GoogleDrive
from src.helpers.database import Firebase
from src.utils.logger import logger

Drive = GoogleDrive()
Database = Firebase()

class DocumentCenter:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DocumentCenter, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
            
        logger.announcement('Initializing Document Center', type='info')
        self.bucket_dictionary = [
            {
                'drive_id': '1tuS0EOHoFm9TiJlv3uyXpbMrSgIKC2QL',
                'id': 'poa',
                'label': 'Proof of Address'
            },
            {
                'drive_id': '1VY0hfcj3EKcDMD6O_d2_gmiKL6rSt_M3',
                'id': 'identity',
                'label': 'Proof of Identity'
            },
            {
                'drive_id': '1WNJkWYWPX6LqWGOTsdq6r1ihAkPJPMHb',
                'id': 'sow',
                'label': 'Source of Wealth'
            },
            {
                'drive_id': '1ik8zbnEJ9fdruy8VPQ59EQqK6ze6cc4-',
                'id': 'deposits',
                'label': 'Deposits and Withdrawals'
            },
            {
                'drive_id': '1-SB4FB1AukcpTMHlDXkfmqTHBOASX8iB',
                'id': 'manifest',
                'label': 'Manifests'
            }
        ]
        logger.announcement('Initialized Document Center', type='success')
        self._initialized = True

    @handle_exception
    def get_folder_dictionary(self):
        return self.bucket_dictionary

    @handle_exception
    def read_files(self, query):
        files = {}

        for folder in self.bucket_dictionary:
            files_in_folder = Database.read(path=f'db/document_center/{folder["id"]}', query=query)
            files[folder['id']] = files_in_folder

        if len(files) == 0:
            raise Exception("No files found")
        
        return files
    
    @handle_exception
    def delete_file(self, document: Document, parent_folder_id: str):
        Database.delete(path=f'db/document_center/{parent_folder_id}', query={'DocumentID': document['DocumentID']})
        Drive.delete_file(document['FileInfo']['id'])
        return {'status': 'success'}
    
    @handle_exception
    def upload_file(self, file_name, mime_type, file_data, parent_folder_id, document_info, uploader, bucket_id):
        file_info = Drive.upload_file(file_name=file_name, mime_type=mime_type, file_data=file_data, parent_folder_id=parent_folder_id)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        document = Document(
            document_id=timestamp,
            document_info=document_info,
            file_info=file_info,
            uploader=uploader
        )

        print(document.to_dict())
        
        Database.create(data=document.to_dict(), path=f'db/document_center/{bucket_id}', id=timestamp)
        return {'status': 'success'}