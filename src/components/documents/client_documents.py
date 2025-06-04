from src.utils.managers.document_center import DocumentCenter

documents = DocumentCenter('clients')

def read(query: dict = None) -> list:
    return documents.read(query=query)

def delete(document: dict = None, bucket_id: str = None) -> str:
    return documents.delete(document=document, bucket_id=bucket_id)

def upload_poa(f: dict = None, document_info: dict = None, user_id: str = None, account_id: str = None) -> str:
    document_info['account_id'] = account_id
    return documents.upload(f=f, document_info=document_info, user_id=user_id, bucket_id='address')

def upload_poi(f: dict = None, document_info: dict = None, user_id: str = None, account_id: str = None) -> str:
    document_info['account_id'] = account_id
    return documents.upload(f=f, document_info=document_info, user_id=user_id, bucket_id='identity')