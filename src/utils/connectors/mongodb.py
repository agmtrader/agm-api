from pymongo import MongoClient
from bson.objectid import ObjectId
import uuid
from src.utils.logger import logger
from src.utils.managers.secret_manager import get_secret

class MongoDB:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDB, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        logger.announcement('Initializing MongoDB connection.', type='info')
        try:
            connection_uri = get_secret("MONGODB_ATLAS_URI")
            self.client = MongoClient(connection_uri)
            self.db = self.client["applications"]
            logger.announcement('Initialized MongoDB connection.', type='success')
        except Exception as e:
            raise Exception(f"Error initializing MongoDB: {e}")
        
        self._initialized = True

    def listCollections(self):
        logger.info("Listing collections.")
        return self.db.list_collection_names()

    def create(self, data, collection_name):
        logger.info(f'Adding document to collection: {collection_name}')
        if not collection_name:
            raise ValueError("Collection name cannot be empty")
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary")
        result = self.db[collection_name].insert_one(data)
        logger.success(f'Document added with _id: {result.inserted_id}')
        return str(result.inserted_id)

    def read(self, collection_name, query=None):
        logger.info(f'Reading from collection: {collection_name} with query: {query}')
        if not collection_name:
            raise ValueError("Collection name cannot be empty")
        if query is None:
            query = {}
        
        # Convert id field to _id ObjectId if present in query
        if 'id' in query:
            try:
                query['_id'] = ObjectId(query['id'])
                del query['id']
            except Exception as e:
                logger.error(f'Error converting id to ObjectId in query: {e}')
                return []
        
        docs = list(self.db[collection_name].find(query))
        for doc in docs:
            doc['id'] = str(doc.get('_id'))
            del doc['_id']
        logger.info(f'Retrieved {len(docs)} documents.')
        return docs

    def find_by_id(self, collection_name, id_value):
        """Find a document by its _id field"""
        logger.info(f'Finding document by _id: {id_value} in collection: {collection_name}')
        if not collection_name:
            raise ValueError("Collection name cannot be empty")
        if not id_value:
            raise ValueError("ID value cannot be empty")
        
        try:
            if isinstance(id_value, str):
                id_value = ObjectId(id_value)
            return self.read(collection_name, {"_id": id_value})
        except Exception as e:
            logger.error(f'Error converting to ObjectId: {e}')
            return []

    def update(self, collection_name, query, update_data):
        logger.info(f'Updating documents in collection: {collection_name} with query: {query}')
        if not query or not update_data:
            raise ValueError("Query and update_data cannot be empty")

        result = self.db[collection_name].update_many(query, {"$set": update_data})
        logger.success(f'{result.modified_count} documents updated.')
        return result.modified_count

    def update_by_id(self, collection_name, id_value, update_data):
        """Update a document by its _id field"""
        logger.info(f'Updating document by _id: {id_value} in collection: {collection_name}')
        if not id_value:
            raise ValueError("ID value cannot be empty")
        
        try:
            if isinstance(id_value, str):
                id_value = ObjectId(id_value)
            return self.update(collection_name, {"_id": id_value}, update_data)
        except Exception as e:
            logger.error(f'Error converting to ObjectId: {e}')
            return 0

    def delete(self, collection_name, query):
        logger.info(f'Deleting documents in collection: {collection_name} with query: {query}')
        if not query:
            raise ValueError("Query cannot be empty")

        result = self.db[collection_name].delete_many(query)
        logger.success(f'{result.deleted_count} documents deleted.')
        return result.deleted_count

    def delete_by_id(self, collection_name, id_value):
        """Delete a document by its _id field"""
        logger.info(f'Deleting document by _id: {id_value} in collection: {collection_name}')
        if not id_value:
            raise ValueError("ID value cannot be empty")
        
        try:
            if isinstance(id_value, str):
                id_value = ObjectId(id_value)
            return self.delete(collection_name, {"_id": id_value})
        except Exception as e:
            logger.error(f'Error converting to ObjectId: {e}')
            return 0
