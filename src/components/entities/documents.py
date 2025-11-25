from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db

@handle_exception
def read_documents(strip_data: bool = False) -> dict:
    """
    Read all documents and their account associations.

    Args:
        strip_data: If True, omit the heavy `data` field from the documents query
                    for faster responses (similar to `strip_application` in
                    applications service).

    Returns:
        Tuple[List[dict], List[dict]]: documents and account_documents lists.
    """
    account_documents = db.read(table='account_document', query={})
    exclude = ['data'] if strip_data else None
    documents = db.read(table='document', query={}, exclude_columns=exclude)

    return documents, account_documents

@handle_exception
def get_document_data(document_id: str = None) -> dict:
    """
    Get the data of a document
    Args:
        document_id: The ID of the document to get the data of
    Returns:
        The data of the document
    """
    return db.read(table='document', query={'id': document_id})