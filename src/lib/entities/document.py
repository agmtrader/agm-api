class Document:
    def __init__(self, document_id: str, document_info: dict, file_info: dict, uploader: str, category: str):
        self.document_id = document_id
        self.document_info = document_info
        self.file_info = file_info
        self.uploader = uploader
        self.category = category

    def to_dict(self):
        return {
            'DocumentID': self.document_id,
            'DocumentInfo': self.document_info,
            'FileInfo': self.file_info,
            'Uploader': self.uploader,
            'Category': self.category
        }