import pandas as pd

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

import uuid

from src.utils.logger import logger
from src.utils.managers.secret_manager import get_secret

class Firebase:
  _instance = None

  def __new__(cls):
    if cls._instance is None:
      cls._instance = super(Firebase, cls).__new__(cls)
      cls._instance._initialized = False
    return cls._instance

  def __init__(self):
    if self._initialized:
      return
      
    logger.announcement('Initializing Firebase connection.', type='info')
    try:
      secret = get_secret('FIREBASE_SERVICE_ACCOUNT')
      cred = credentials.Certificate(secret)
      firebase_admin.initialize_app(cred)
      logger.announcement('Initialized Firebase connection.', type='success')
    except Exception as e:
      try:
        firebase_admin.get_app()
      except:
        raise Exception(e)
    self.db = firestore.client()
    self._initialized = True

  def listSubcollections(self, path):
    logger.info(f'Listing subcollections in document: {path}')
    if not path:
        raise ValueError("Path cannot be empty")
    
    # Get a reference to the document
    doc_ref = self.db.document(path)
    
    # List the subcollections of the document
    collections = doc_ref.collections()
    
    results = []
    for collection in collections:
        # For each subcollection, get its documents
        docs = collection.stream()
        subcollection_data = []
        for doc in docs:
            doc_dict = doc.to_dict()
            doc_dict['id'] = doc.id
            subcollection_data.append(doc_dict)
        
        results.append({
            'collection_id': collection.id,
            'documents': subcollection_data
        })
    
    logger.success(f'Successfully listed subcollections.')
    return results

  def create(self, data, path):
    logger.info(f'Adding document to collection: {path}')
    if not path:
      raise ValueError("Path cannot be empty")
    if not data or not isinstance(data, dict):
      raise ValueError("Data must be a non-empty dictionary")
    
    created_id = str(uuid.uuid4())
    data['id'] = created_id

    self.db.collection(path).document(created_id).set(data)
    logger.success(f'Document added successfully.')
    return created_id

  def read(self, path, query=None):
    logger.info(f'Querying documents in collection: {path} with query: {query}')
    if not path:
      raise ValueError("Path cannot be empty")
    
    ref = self.db.collection(path)
    if query:
      if not isinstance(query, dict):
        raise TypeError("Query must be a dictionary")
      for key, value in query.items():
        if not isinstance(key, str):
          raise TypeError("Query keys must be strings")
        ref = ref.where(filter=firestore.FieldFilter(key, "==", value))
    
    results = []
    for doc in ref.stream():
        doc_dict = doc.to_dict()
        doc_dict['id'] = doc.id  # Add the document ID to the dictionary
        results.append(doc_dict)
    
    logger.info(f'Retrieved {len(results)} documents.')
    return results

  def update(self, path, data, query=None):
    """
    Updates a document in the database.

    Args:
      path (str): The path to the collection.
      data (dict): The data to update. Does not need to be the entire entity, only the properties you want to update.
      query (dict): The query to filter the documents. All documents that match the query will be updated.

    Returns:
      dict: The updated documents count.
    """

    if query is None:
      raise ValueError("Query cannot be empty")

    logger.info(f'Updating documents in collection: {path} with query: {query}')
    if not path:
      raise ValueError("Path cannot be empty")
    if not data or not isinstance(data, dict):
      raise ValueError("Data must be a non-empty dictionary")
    
    ref = self.db.collection(path)
    if query:
      if not isinstance(query, dict):
        raise TypeError("Query must be a dictionary")
      for key, value in query.items():
        if not isinstance(key, str):
          raise TypeError("Query keys must be strings")
        ref = ref.where(filter=firestore.FieldFilter(key, "==", value))
    
    batch = self.db.batch()
    docs = ref.stream()
    updated_count = 0
    for doc in docs:
      batch.update(doc.reference, data)
      updated_count += 1
    
    batch.commit()
    updated_id = ref.document().id
    logger.success(f'{updated_count} documents updated successfully.')
    return updated_id

  def delete(self, path, query=None):
    """
    Deletes documents from the database.

    Args:
      path (str): The path to the collection.
      query (dict): The query to filter the documents.

    Returns:
      dict: The number of documents deleted.
    """

    if query is None:
      raise ValueError("Query cannot be empty")

    logger.info(f'Deleting documents in collection: {path} with query: {query}')
    if not path:
      raise ValueError("Path cannot be empty")
    
    ref = self.db.collection(path)
    if query:
      if not isinstance(query, dict):
        raise TypeError("Query must be a dictionary")
      for key, value in query.items():
        if not isinstance(key, str):
          raise TypeError("Query keys must be strings")
        ref = ref.where(filter=firestore.FieldFilter(key, "==", value))
    
    batch = self.db.batch()
    docs = ref.stream()
    deleted_count = 0
    for doc in docs:
      batch.delete(doc.reference)
      deleted_count += 1
      if deleted_count % 100 == 0:
        logger.info(f'Deleting {deleted_count} documents.')
    
    batch.commit()
    deleted_id = ref.document().id
    logger.success(f'Deleted {deleted_count} documents.')
    return deleted_id
