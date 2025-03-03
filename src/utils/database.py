from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from datetime import datetime
from functools import wraps

from src.utils.logger import logger
from src.utils.response import Response

class DatabaseHandler:
    
    def __init__(self, base: declarative_base, engine: create_engine, type: str = 'sqlite'):
        """
        Initialize the DatabaseHandler class.

        Args:
            base (declarative_base): The base class for the database models.
            with_session (function): The function to wrap database operations.
            db_name (str): The name of the database.
        """
        self.engine = engine
        self.type = type
        self.base = base
        
        try:
            self.base.metadata.create_all(self.engine)
        except Exception as e:
            logger.error(f'Error creating tables: {str(e)}')

        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        logger.success(f'Database initialized')

    def with_session(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if self.type == 'sqlite':
                session = Session(bind=self.engine)
            else:
                session = Session()
            try:
                result = func(session, *args, **kwargs)
                session.commit()
                return result
            except Exception as e:
                session.rollback()
                logger.error(f"Database error in {func.__name__}: {str(e)}")
                return Response.error(f"Database error: {str(e)}")
            finally:
                session.close()
        return wrapper

    def create(self, table: str, data: dict):
        @self.with_session
        def _create(session, table: str, data: dict):
            logger.info(f'Attempting to create new entry in table: {table}')

            try:
                tbl = Table(table, self.metadata, autoload_with=self.engine)
                current_time = datetime.now()
                data = {
                    'created': current_time,
                    'updated': current_time,
                    **data
                }
                new_record = tbl.insert().values(**data)
                result = session.execute(new_record)
                session.flush()
                new_id = result.inserted_primary_key[0]
                logger.success(f'Successfully created entry with id: {new_id}')
                return Response.success(new_id)
            
            except SQLAlchemyError as e:
                logger.error(f'Error creating record: {str(e)}')
                return Response.error(f'Database error: {str(e)}')

        return _create(table, data)

    def read(self, table: str, params: dict = None):
        @self.with_session
        def _read(session, table: str, params: dict = None):
            logger.info(f'Attempting to read entry from table: {table}')
            
            try:
                tbl = Table(table, self.metadata, autoload_with=self.engine)
                query = session.query(tbl)

                if params:
                    for key, value in params.items():
                        if hasattr(tbl.c, key):
                            query = query.filter(getattr(tbl.c, key) == value)
                    
                results = query.all()

                serialized_results = [row._asdict() for row in results]
                
                logger.success(f'Successfully read {len(serialized_results)} entries from table: {table}')
                return Response.success(serialized_results)
            
            except SQLAlchemyError as e:
                logger.error(f'Error reading from database: {str(e)}')
                return Response.error(f'Database error: {str(e)}')

        return _read(table, params)

    def update(self, table: str, params: dict, data: dict):
        @self.with_session
        def _update(session, table: str, params: dict, data: dict):
            logger.info(f'Attempting to update entry in table: {table}')
            
            try:
                tbl = Table(table, self.metadata, autoload_with=self.engine)
                query = session.query(tbl)

                for key, value in params.items():
                    if hasattr(tbl.c, key):
                        query = query.filter(getattr(tbl.c, key) == value)

                item = query.first()

                if not item:
                    return Response.error(f"{table.capitalize()} with given parameters not found")
                
                logger.info(f'Updating entry timestamp.')
                data['updated'] = datetime.now()

                query.update(data)
                session.flush()

                updated_item = query.first()
                logger.success(f"Successfully updated entry with id: {updated_item.id} in table: {table}.")

                serialized_item = updated_item._asdict()
                return Response.success(serialized_item)
            
            except SQLAlchemyError as e:
                logger.error(f"Error updating {table}: {str(e)}")
                return Response.error(f'Database error: {str(e)}')

        return _update(table, params, data)

    def delete(self, table: str, params: dict):
        @self.with_session
        def _delete(session, table: str, params: dict):
            logger.info(f'Attempting to delete entry from table: {table}')
            try:
                tbl = Table(table, self.metadata, autoload_with=self.engine)
                query = session.query(tbl)

                for key, value in params.items():
                    if hasattr(tbl.c, key):
                        query = query.filter(getattr(tbl.c, key) == value)

                item = query.first()
                if not item:
                    return Response.error(f"Entry with given parameters not found in table: {table}.")

                delete_stmt = tbl.delete().where(tbl.c.id == item.id)
                session.execute(delete_stmt)
                session.flush()

                logger.success(f"Successfully deleted entry with id: {item.id} from table: {table}.")
                return Response.success(item)
            
            except SQLAlchemyError as e:
                logger.error(f"Error deleting {table}: {str(e)}")
                return Response.error(f'Database error: {str(e)}')

        return _delete(table, params)
    
    def delete_all(self, table: str):
        @self.with_session
        def _delete_all(session, table: str):
            logger.info(f'Attempting to delete all entries from table: {table}')
            try:
                tbl = Table(table, self.metadata, autoload_with=self.engine)
                session.execute(tbl.delete())
                session.flush()
                logger.success(f'Successfully deleted all entries from table: {table}')
                return Response.success(f'Successfully deleted all entries from table: {table}')
            except SQLAlchemyError as e:
                logger.error(f'Error deleting all entries from table: {str(e)}')
                return Response.error(f'Database error: {str(e)}')

        return _delete_all(table)

    def get_tables(self):
        @self.with_session
        def _get_tables(session):
            """Returns a list of all tables in the database."""
            logger.info('Attempting to get all tables from database')
            try:
                table_names = self.metadata.tables.keys()
                logger.success(f'Successfully retrieved {len(table_names)} tables')
                return Response.success(list(table_names))
            except SQLAlchemyError as e:
                logger.error(f'Error getting tables: {str(e)}')
                return Response.error(f'Database error: {str(e)}')

        return _get_tables()

    def get_schema(self, table: str):
        """Returns the schema of a specified table."""
        logger.info(f'Attempting to get schema for table: {table}')
        try:
            if table not in self.metadata.tables:
                return Response.error(f"Table '{table}' not found in database")
            
            tbl = self.metadata.tables[table]
            schema = {}
            
            for column in tbl.columns:
                schema[column.name] = {
                    'type': str(column.type),
                    'nullable': column.nullable,
                    'primary_key': column.primary_key,
                    'default': str(column.default) if column.default else None,
                    'foreign_keys': [str(fk.target_fullname) for fk in column.foreign_keys]
                }
            
            logger.success(f'Successfully retrieved schema for table: {table}')
            return Response.success(schema)
        
        except SQLAlchemyError as e:
            logger.error(f'Error getting schema: {str(e)}')
            return Response.error(f'Database error: {str(e)}')

    def from_data_object(self, data: dict, table: str, overwrite: bool = False):
        """
        Imports a data object to a SQLite table.
        The data object must be a list of dictionaries [{}, {}, {}]. (pd.DataFrame.to_dict('records') format)
        Recieves data and destination table name, and imports the data to the table.
        If overwrite is True, the table will be truncated before the data is imported.
        If the table does not exist, it will be created.
        """
        @self.with_session
        def _from_data_object(session, data: dict, table: str, overwrite: bool):
            logger.info(f'Attempting to import data to table: {table}')
            try:
                if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
                    return Response.error("Data must be a list of dictionaries")

                tbl = Table(table, self.metadata, autoload_with=self.engine)

                if overwrite:
                    logger.info(f'Truncating table: {table}')
                    session.execute(tbl.delete())

                if not data:
                    logger.warning(f'No data to import to table: {table}')
                    return Response.success("No data to import")

                current_time = datetime.now()
                for item in data:
                    item['created'] = current_time
                    item['updated'] = current_time

                session.execute(tbl.insert(), data)
                session.flush()
                
                count = len(data)
                logger.success(f'Successfully imported {count} records to table: {table}')
                return Response.success(f"Successfully imported {count} records")

            except SQLAlchemyError as e:
                logger.error(f'Error importing data: {str(e)}')
                return Response.error(f'Database error: {str(e)}')

        return _from_data_object(data, table, overwrite)