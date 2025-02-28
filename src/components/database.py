import os
import pandas as pd

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from src.utils.logger import logger
from src.utils.response import Response
from src.utils.exception import handle_exception
from flask import jsonify



class Firebase:

  def __init__(self):
    logger.announcement('Initializing Firebase connection.', type='info')
    try:
      cred = credentials.Certificate({
          "type": os.getenv('FIREBASE_TYPE'),
          "project_id": os.getenv('FIREBASE_PROJECT_ID'),
          "private_key_id": os.getenv('FIREBASE_PRIVATE_KEY_ID'),
          "private_key": os.getenv('FIREBASE_PRIVATE_KEY').replace('"', '').replace('\\n', '\n'),
          "client_email": os.getenv('FIREBASE_CLIENT_EMAIL'),
          "client_id": os.getenv('FIREBASE_CLIENT_ID'),
          "auth_uri": os.getenv('FIREBASE_AUTH_URI'),
          "token_uri": os.getenv('FIREBASE_TOKEN_URI'),
          "auth_provider_x509_cert_url": os.getenv('FIREBASE_AUTH_PROVIDER_X509_CERT_URL'),
          "client_x509_cert_url": os.getenv('FIREBASE_CLIENT_X509_CERT_URL'),
          "universe_domain": os.getenv('FIREBASE_UNIVERSE_DOMAIN')
      })
      firebase_admin.initialize_app(cred)
      logger.announcement('Initialized Firebase connection.', type='success')
    except Exception as e:
      try:
        firebase_admin.get_app()
      except:
        raise Exception(e)
    self.db = firestore.client()

  # listing subcollections basically lists all the csv files in a folder
  def listSubcollections(self, path):
    logger.info(f'Listing subcollections in document: {path}')
    try:
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
        return Response.success(results)
    except Exception as e:
        return Response.error(f"Error listing subcollections: {str(e)}")

  # collections are basically csv documents
  def clear_collection(self, path):
    logger.info(f'Clearing collection: {path}')
    try:
      docs = self.db.collection(path).list_documents()
      for i, doc in enumerate(docs):
        doc.delete()
        if i != 0:
          if i % 10 == 0:
            logger.info(f'Deleted {i} documents.')
          elif i % 100 == 0:
            logger.announcement(f'Deleted {i} documents.', type='info')
      logger.success(f'Collection cleared successfully.')
      return Response.success(f'Collection cleared successfully.')
    except Exception as e: 
      logger.error(f"Error clearing collection: {str(e)}")
      return Response.error(f"Error clearing collection: {str(e)}")
  
  # upload collection is used to upload a list of dictionaries or pandas DataFrame to a folder
  def upload_collection(self, path, data):
    try:
      # Clear the collection before uploading
      self.clear_collection(path)
      
      logger.info(f'Uploading collection: {path}')
      # Convert pandas DataFrame to list of dictionaries if necessary
      if isinstance(data, pd.DataFrame):
        data = data.to_dict('records')
      
      # Iterate through the data and add each row as a document
      for i, row in enumerate(data):
        self.db.collection(path).add(row)
        if i != 0:
          if i % 10 == 0:
            logger.info(f'Uploaded {i} documents.')
          elif i % 100 == 0:
            logger.announcement(f'Uploaded {i} documents.', type='info')
          
      logger.success(f'Collection uploaded successfully.')
      return Response.success(f'Collection uploaded successfully.')
    
    except Exception as e:
      logger.error(f"Error uploading collection: {str(e)}")
      return Response.error(f"Error uploading collection: {str(e)}")

  @handle_exception
  def create(self, data, path, id):
    logger.info(f'Adding document to collection: {path} with id: {id}')
    if not path:
      raise ValueError("Path cannot be empty")
    if not id:
      raise ValueError("ID cannot be empty")
    if not data or not isinstance(data, dict):
      raise ValueError("Data must be a non-empty dictionary")
    
    self.db.collection(path).document(id).set(data)
    logger.success(f'Document added successfully.')
    return jsonify({'id': id})

  @handle_exception
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

  @handle_exception
  def update(self, path, data, query=None):
    logger.info(f'Updating documents in collection: {path} with updates: {data} and query: {query}')
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
    logger.success(f'{updated_count} documents updated successfully.')
    return updated_count

  @handle_exception
  def delete(self, path, query=None):
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
    logger.success(f'Deleted {deleted_count} documents.')
    return Response.success(deleted_count)
