import json
from datetime import datetime
from src.lib.entities.document import Document

from src.utils.exception import handle_exception
from src.helpers.drive import GoogleDrive
from src.helpers.database import Firebase

drive = GoogleDrive()
database = Firebase()


# TODO MAKE THIS A SINGLETON

class DocumentCenter:

    def __init__(self):
        self.default_folder_dictionary = [
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

    @handle_exception
    def get_folder_dictionary(self):
        return self.default_folder_dictionary

    @handle_exception
    def read_files(self, query):
        files = {}

        for folder in self.default_folder_dictionary:
            files_in_folder = database.read(path=f'db/document_center/{folder["id"]}', query=query)
            files[folder['id']] = json.loads(files_in_folder.data.decode("utf-8"))

        if len(files) == 0:
            raise Exception("No files found")
        
        return files
    
    @handle_exception
    def delete_file(self, document: Document, parent_folder_id: str):
        database.delete(path=f'db/document_center/{parent_folder_id}', query={'DocumentID': document['DocumentID']})
        drive.delete_file(document['FileInfo']['id'])
        return {'status': 'success'}
    
    @handle_exception
    def upload_file(self, file_name, mime_type, file_data, parent_folder_id, document_info, uploader):

        file_info = drive.upload_file(file_name=file_name, mime_type=mime_type, file_data=file_data, parent_folder_id=parent_folder_id)
        file_info = json.loads(file_info.data.decode("utf-8"))
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        for folder in self.default_folder_dictionary:
            if folder['drive_id'] == parent_folder_id:
                category = folder['id']
                break
            else:
                category = 'other'

        document = Document(
            document_id=timestamp,
            document_info=document_info,
            file_info=file_info,
            uploader=uploader,
            category=category
        )
        
        database.create(data=document.to_dict(), path=f'db/document_center/{category}', id=timestamp)
        return {'status': 'success'}