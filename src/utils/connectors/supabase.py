from sqlalchemy import Boolean, ForeignKey, Text, create_engine, Column, ARRAY, MetaData
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.automap import automap_base

import uuid
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

from src.utils.managers.database_manager import DatabaseManager
from src.utils.logger import logger
from src.utils.managers.secret_manager import get_secret

class Supabase:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Supabase, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            logger.announcement('Initializing Database Service', 'info')

            supabase_user = get_secret('SUPABASE_USER')
            supabase_password = get_secret('SUPABASE_PASSWORD')
            
            self.db_url = f'postgresql://postgres.{supabase_user}:{supabase_password}@aws-0-us-west-1.pooler.supabase.com:6543/postgres?gssencmode=disable'
            self.engine = create_engine(self.db_url)
            
            self.Base = declarative_base()
            self._setup_models()
            
            self.db = DatabaseManager(base=self.Base, engine=self.engine)
            
            logger.announcement('Successfully initialized Database Service', 'success')
            self._initialized = True

    def _setup_models(self):

        class Contact(self.Base):
            __tablename__ = 'contact'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            country = Column(Text, nullable=True)
            name = Column(Text, nullable=False)
            company_name = Column(Text, nullable=True)
            email = Column(Text, nullable=True, unique=True)
            phone = Column(Text, nullable=True, unique=True)

        class User(self.Base):
            __tablename__ = 'user'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            email = Column(Text, nullable=False, unique=True)
            image = Column(Text, nullable=True)
            password = Column(Text, nullable=False)
            scopes = Column(Text, nullable=False)
            name = Column(Text, nullable=False)

        class Advisor(self.Base):
            __tablename__ = 'advisor'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            code = Column(Text, nullable=False)
            agency = Column(Text, nullable=False)
            hierarchy1 = Column(Text, nullable=False)
            hierarchy2 = Column(Text, nullable=False)
            name = Column(Text, nullable=False)

        class Lead(self.Base):
            __tablename__ = 'lead'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            referrer_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            description = Column(Text, nullable=True)
            status = Column(Text, nullable=False)
            completed = Column(Boolean, nullable=False, default=False)
            contact_date = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            application_id = Column(UUID(as_uuid=True), nullable=True)

        class FollowUp(self.Base):
            __tablename__ = 'follow_up'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            lead_id = Column(UUID(as_uuid=True), ForeignKey('lead.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            description = Column(Text, nullable=True)
            completed = Column(Boolean, nullable=False, default=False)
            date = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))

        class Account(self.Base):
            __tablename__ = 'account'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            advisor_id = Column(UUID(as_uuid=True), ForeignKey('advisor.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            user_id = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            lead_id = Column(UUID(as_uuid=True), ForeignKey('lead.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            master_account_id = Column(Text, nullable=True)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            status = Column(Text, nullable=False)
            account_type = Column(Text, nullable=False)
            ibkr_account_number = Column(Text, nullable=True)

        class Document(self.Base):
            __tablename__ = 'document'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            user_id = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            drive_id = Column(Text, nullable=False)
            mime_type = Column(Text, nullable=False)
            name = Column(Text, nullable=False)
            parents = Column(ARRAY(Text), nullable=False)

        class POADocument(self.Base):
            __tablename__ = 'poa_document'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            document_id = Column(UUID(as_uuid=True), ForeignKey('document.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            type = Column(Text, nullable=False)
            issued_date = Column(Text, nullable=False)

        class POIDocument(self.Base):
            __tablename__ = 'poi_document'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            document_id = Column(UUID(as_uuid=True), ForeignKey('document.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            gender = Column(Text, nullable=False)
            country_of_issue = Column(Text, nullable=False)
            type = Column(Text, nullable=False)
            full_name = Column(Text, nullable=False)
            id_number = Column(Text, nullable=False)
            issued_date = Column(Text, nullable=False)
            date_of_birth = Column(Text, nullable=False)
            expiration_date = Column(Text, nullable=False)
            country_of_birth = Column(Text, nullable=False)

        class AccountRiskProfile(self.Base):
            __tablename__ = 'account_risk_profile'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            risk_profile_id = Column(Text, nullable=False)
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            score = Column(Text, nullable=False)

        class TradeTicket(self.Base):
            __tablename__ = 'trade_ticket'
            id = Column(Text, primary_key=True)
            user_id = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            name = Column(Text, nullable=False)

        # Contacts
        self.Contact = Contact
        self.User = User
        self.Advisor = Advisor

        # Leads
        self.Lead = Lead
        self.FollowUp = FollowUp

        # Accounts
        self.Account = Account

        # Documents
        self.Document = Document
        self.POADocument = POADocument
        self.POIDocument = POIDocument

        # Risk Profiles
        self.AccountRiskProfile = AccountRiskProfile

        # Trade Tickets
        self.TradeTicket = TradeTicket

# Create a single instance that can be imported and used throughout the application
db = Supabase().db