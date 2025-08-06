from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from datetime import datetime
from functools import wraps
from flask import jsonify
from src.utils.logger import logger
from src.utils.exception import handle_exception
import re
from sqlalchemy import inspect

class DatabaseManager:
    
    def __init__(self, base: declarative_base, engine: create_engine):
        """
        Initialize the DatabaseHandler class.

        Args:
            base (declarative_base): The base class for the database models.
            with_session (function): The function to wrap database operations.
            db_name (str): The name of the database.
        """
        self.engine = engine
        self.base = base
        
        # First get all tables from the database
        inspector = inspect(self.engine)
        db_tables = inspector.get_table_names()
        
        # Get all models from SQLAlchemy Base
        model_tables = self.base.metadata.tables.keys()
        
        # Compare and log differences
        logger.info("Validating database schema...")
        
        # Check for missing tables in models
        missing_in_models = set(db_tables) - set(model_tables)
        if missing_in_models:
            logger.error(f"Tables in database but missing in models: {missing_in_models}")
            raise Exception(f"Tables in database but missing in models: {missing_in_models}")
        
        # Check for extra tables in models
        extra_in_models = set(model_tables) - set(db_tables)
        if extra_in_models:
            logger.error(f"Tables in models but missing in database: {extra_in_models}")
            raise Exception(f"Tables in models but missing in database: {extra_in_models}")
        
        # Check detailed column differences for each table
        for table_name in set(db_tables) & set(model_tables):
            self._validate_table_schema(inspector, table_name)
        
        try:
            self.base.metadata.create_all(self.engine)
        except Exception as e:
            logger.error(f'Error creating tables: {str(e)}')
            raise Exception(f'Error creating tables: {str(e)}')

        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        logger.success(f'Database initialized')

    def _validate_table_schema(self, inspector, table_name):
        """Validate detailed schema differences for a specific table"""
        db_columns = {col['name']: col for col in inspector.get_columns(table_name)}
        model_columns = {col.name: col for col in self.base.metadata.tables[table_name].columns}
        
        # Check for missing columns in models
        missing_cols = set(db_columns.keys()) - set(model_columns.keys())
        if missing_cols:
            logger.error(f"Table '{table_name}' missing columns in model: {missing_cols}")
            raise Exception(f"Table '{table_name}' missing columns in model: {missing_cols}")
        
        # Check for extra columns in models
        extra_cols = set(model_columns.keys()) - set(db_columns.keys())
        if extra_cols:
            logger.error(f"Table '{table_name}' has extra columns in model: {extra_cols}")
            raise Exception(f"Table '{table_name}' has extra columns in model: {extra_cols}")
        
        # Get primary keys and foreign keys from database
        db_pk_columns = set(inspector.get_pk_constraint(table_name)['constrained_columns'])
        db_foreign_keys = {fk['constrained_columns'][0]: fk for fk in inspector.get_foreign_keys(table_name)}
        db_unique_constraints = []
        for constraint in inspector.get_unique_constraints(table_name):
            db_unique_constraints.extend(constraint['column_names'])
        
        # Validate each column's detailed properties
        for col_name in set(db_columns.keys()) & set(model_columns.keys()):
            db_col = db_columns[col_name]
            model_col = model_columns[col_name]
            
            # Check primary key consistency
            is_db_pk = col_name in db_pk_columns
            is_model_pk = model_col.primary_key
            if is_db_pk != is_model_pk:
                logger.error(f"Table '{table_name}', column '{col_name}': Primary key mismatch. DB: {is_db_pk}, Model: {is_model_pk}")
                raise Exception(f"Table '{table_name}', column '{col_name}': Primary key mismatch")
            
            # Check nullable consistency
            db_nullable = db_col.get('nullable', True)
            model_nullable = model_col.nullable
            if db_nullable != model_nullable:
                logger.error(f"Table '{table_name}', column '{col_name}': Nullable mismatch. DB: {db_nullable}, Model: {model_nullable}")
                raise Exception(f"Table '{table_name}', column '{col_name}': Nullable mismatch")
            
            # Check foreign key consistency
            has_db_fk = col_name in db_foreign_keys
            has_model_fk = len(model_col.foreign_keys) > 0
            if has_db_fk != has_model_fk:
                logger.error(f"Table '{table_name}', column '{col_name}': Foreign key mismatch. DB: {has_db_fk}, Model: {has_model_fk}")
                raise Exception(f"Table '{table_name}', column '{col_name}': Foreign key mismatch")
            
            # If both have foreign keys, check if they reference the same table/column
            if has_db_fk and has_model_fk:
                db_fk = db_foreign_keys[col_name]
                db_ref_table = db_fk['referred_table']
                db_ref_column = db_fk['referred_columns'][0]
                
                model_fk = list(model_col.foreign_keys)[0]
                model_ref_parts = str(model_fk.target_fullname).split('.')
                model_ref_table = model_ref_parts[0]
                model_ref_column = model_ref_parts[1] if len(model_ref_parts) > 1 else 'id'
                
                if db_ref_table != model_ref_table or db_ref_column != model_ref_column:
                    logger.error(f"Table '{table_name}', column '{col_name}': Foreign key reference mismatch. DB: {db_ref_table}.{db_ref_column}, Model: {model_ref_table}.{model_ref_column}")
                    raise Exception(f"Table '{table_name}', column '{col_name}': Foreign key reference mismatch")
            
            # Check unique constraint consistency
            has_db_unique = col_name in db_unique_constraints
            has_model_unique = model_col.unique or False
            if has_db_unique != has_model_unique:
                logger.error(f"Table '{table_name}', column '{col_name}': Unique constraint mismatch. DB: {has_db_unique}, Model: {has_model_unique}")
                raise Exception(f"Table '{table_name}', column '{col_name}': Unique constraint mismatch")
            
            # Check data type consistency (basic check)
            db_type_str = str(db_col['type']).upper()
            model_type_str = str(model_col.type).upper()
            
            # Normalize common type differences
            type_mappings = {
                'VARCHAR': 'TEXT',
                'CHARACTER VARYING': 'TEXT',
                'BOOLEAN': 'BOOL',
                'TIMESTAMP': 'DATETIME',
                'TIMESTAMP WITHOUT TIME ZONE': 'DATETIME'
            }
            
            for old_type, new_type in type_mappings.items():
                db_type_str = db_type_str.replace(old_type, new_type)
                model_type_str = model_type_str.replace(old_type, new_type)
            
            # Remove length specifications for comparison (e.g., VARCHAR(255) -> VARCHAR)
            db_type_base = db_type_str.split('(')[0]
            model_type_base = model_type_str.split('(')[0]
            
            if db_type_base != model_type_base:
                logger.warning(f"Table '{table_name}', column '{col_name}': Data type difference. DB: {db_type_str}, Model: {model_type_str}")
                # Don't raise exception for type differences, just warn as they might be compatible
        
        logger.info(f"Schema validation completed for table '{table_name}'")

    def with_session(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            session = Session(bind=self.engine)
            try:
                result = func(session, *args, **kwargs)
                session.commit()
                return result
            except Exception as e:
                session.rollback()
                logger.error(f"Database error in {func.__name__}: {str(e)}")
                raise Exception(f"Database error: {str(e)}")
            finally:
                session.close()
        return wrapper

    def _ids_to_string(self, data: dict):
        for key, value in data.items():
            if re.match(r'^id$|^\w+_id$', key):
                data[key] = str(value)
        return data
    
    def _none_to_null(self, data: dict):
        for key, value in data.items():
            if value == 'None':
                data[key] = None
        return data

    def _dates_to_timestamp(self, data: dict):
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.strftime('%Y%m%d%H%M%S')
            elif isinstance(value, str):
                try:
                    data[key] = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y%m%d%H%M%S')
                except:
                    pass
        return data

    def create(self, table: str, data: dict = None) -> str:
        @self.with_session
        def _create(session, table: str, data: dict = None):
            logger.info(f'Attempting to create new entry in table: {table}')

            if not data:
                raise Exception("Data to create must be provided.")
            
            # Get the model class for the table
            model = None
            for class_ in self.base.__subclasses__():
                if hasattr(class_, '__tablename__') and class_.__tablename__ == table:
                    model = class_
                    break
            
            if not model:
                raise Exception(f"Model not found for table: {table}")
            
            current_time = datetime.now().strftime('%Y%m%d%H%M%S')
            data = self._dates_to_timestamp(data)

            data = {
                'created': current_time,
                'updated': current_time,
                **data
            }
            
            # Create a new instance of the model
            new_record = model(**data)
            session.add(new_record)
            session.flush()
            
            logger.success(f'Successfully created entry with id: {new_record.id}')
            return str(new_record.id)

        return _create(table, data)

    def read(self, table: str, query: dict = None) -> list:
        @self.with_session
        def _read(session, table: str, query: dict = None):

            logger.info(f'Attempting to read entry from table: {table} with query: {query}')

            if query is None:
                raise Exception("Query must be provided.")
            
            tbl = Table(table, self.metadata, autoload_with=self.engine)
            sql_query = session.query(tbl)

            if query:
                for key, value in query.items():
                    if hasattr(tbl.c, key):
                        sql_query = sql_query.filter(getattr(tbl.c, key) == value)
                
            compiled_query = sql_query.statement.compile(
                compile_kwargs={"literal_binds": True},
                dialect=self.engine.dialect
            )
            logger.info(f'Generated SQL: {str(compiled_query)}')
            
            results = sql_query.all()

            serialized_results = [row._asdict() for row in results]

            for result in serialized_results:
                self._ids_to_string(result)
                self._none_to_null(result)
            logger.success(f'Successfully read {len(serialized_results)} entries from table: {table}')
            return serialized_results

        return _read(table, query)

    def update(self, table: str, query: dict = None, data: dict = None) -> str:
        @self.with_session
        def _update(session, table: str, query: dict = None, data: dict = None):
            logger.info(f'Attempting to update entry in table: {table}')

            if query is None:
                raise Exception("Query must be provided.")
            
            if data is None:
                raise Exception("Data to update must be provided.")

            tbl = Table(table, self.metadata, autoload_with=self.engine)
            sql_query = session.query(tbl)

            for key, value in query.items():
                if hasattr(tbl.c, key):
                    sql_query = sql_query.filter(getattr(tbl.c, key) == value)

            item = sql_query.first()

            if not item:
                raise Exception(f"{table.capitalize()} with given parameters not found")
            
            logger.info(f'Updating entry timestamp.')
            data['updated'] = datetime.now().strftime('%Y%m%d%H%M%S')
            data = self._dates_to_timestamp(data)

            sql_query.update(data)
            session.flush()

            updated_item = sql_query.first()
            logger.success(f"Successfully updated entry with id: {updated_item.id} in table: {table}.")
            
            return str(updated_item.id)

        return _update(table, query, data)

    def delete(self, table: str, query: dict = None) -> str:
        @self.with_session
        def _delete(session, table: str, query: dict = None):
            logger.info(f'Attempting to delete entry from table: {table}')

            if query is None:
                raise Exception("Query must be provided.")
            
            tbl = Table(table, self.metadata, autoload_with=self.engine)
            sql_query = session.query(tbl)

            if query:
                for key, value in query.items():
                    if hasattr(tbl.c, key):
                        sql_query = sql_query.filter(getattr(tbl.c, key) == value)

            item = sql_query.first()
            if not item:
                raise Exception(f"Entry with given parameters not found in table: {table}.")

            delete_stmt = tbl.delete().where(tbl.c.id == item.id)
            session.execute(delete_stmt)
            session.flush()

            logger.success(f"Successfully deleted entry with id: {item.id} from table: {table}.")
            return str(item.id)

        return _delete(table, query)
        
    def get_tables(self):
        @self.with_session
        def _get_tables(session):
            """Returns a list of all tables in the database."""
            logger.info('Attempting to get all tables from database')
            table_names = self.metadata.tables.keys()
            logger.success(f'Successfully retrieved {len(table_names)} tables')
            return list(table_names)

        return _get_tables()

    @handle_exception
    def get_schema(self, table: str):
        """Returns the schema of a specified table."""
        logger.info(f'Attempting to get schema for table: {table}')
        if table not in self.metadata.tables:
            raise Exception(f"Table '{table}' not found in database")
        
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
        return schema

    def from_data_object(self, data: dict, table: str, overwrite: bool = False):
        """
        Imports a data object to a SQLite table.
        The data object must be a list of dictionaries [{}, {}, {}]. (pd.DataFrame.to_dict('records') format)
        Recieves data and destination table name, and imports the data to the table.
        If overwrite is True, the table will be truncated before the data is imported.
        If the table does not exist, it will be created.
        """
        @self.with_session
        @handle_exception
        def _from_data_object(session, data: dict, table: str, overwrite: bool):
            logger.info(f'Attempting to import data to table: {table}')
            try:
                if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
                    raise Exception("Data must be a list of dictionaries")

                tbl = Table(table, self.metadata, autoload_with=self.engine)

                if overwrite:
                    logger.info(f'Truncating table: {table}')
                    session.execute(tbl.delete())

                if not data:
                    logger.error(f'No data to import to table: {table}')
                    raise Exception(f'No data to import to table: {table}')

                current_time = datetime.now().strftime('%Y%m%d%H%M%S')
                for item in data:
                    item['created'] = current_time
                    item['updated'] = current_time

                session.execute(tbl.insert(), data)
                session.flush()
                
                count = len(data)
                logger.success(f'Successfully imported {count} records to table: {table}')
                return jsonify({'inserted': count})

            except SQLAlchemyError as e:
                logger.error(f'Error importing data: {str(e)}')
                raise Exception(f'Database error: {str(e)}')

        return _from_data_object(data, table, overwrite)