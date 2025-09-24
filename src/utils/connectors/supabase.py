from sqlalchemy import Boolean, ForeignKey, Text, create_engine, Column, Integer
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
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

        class User(self.Base):
            __tablename__ = 'user'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            email = Column(Text, nullable=True, unique=True)
            image = Column(Text, nullable=True)
            password = Column(Text, nullable=False)
            scopes = Column(Text, nullable=True)
            name = Column(Text, nullable=False)
            country = Column(Text, nullable=True)
            company_name = Column(Text, nullable=True)
            phone = Column(Text, nullable=True)
            last_login = Column(Text, nullable=True)

        class Contact(self.Base):
            __tablename__ = 'contact'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            email = Column(Text, nullable=True, unique=True)
            image = Column(Text, nullable=True)
            name = Column(Text, nullable=False)
            country = Column(Text, nullable=True)
            company_name = Column(Text, nullable=True)
            phone = Column(Text, nullable=True)

        class Advisor(self.Base):
            __tablename__ = 'advisor'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            code = Column(Integer, nullable=False, unique=True)
            agency = Column(Text, nullable=False)
            hierarchy1 = Column(Text, nullable=False)
            hierarchy2 = Column(Text, nullable=False)
            name = Column(Text, nullable=False)

        class Lead(self.Base):
            __tablename__ = 'lead'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False, unique=True)
            referrer_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            description = Column(Text, nullable=False)
            contact_date = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            closed = Column(Text, nullable=True, default=None)
            sent = Column(Text, nullable=True, default=None)
            emails_to_notify = Column(ARRAY(Text), nullable=True)

        class FollowUp(self.Base):
            __tablename__ = 'follow_up'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            lead_id = Column(UUID(as_uuid=True), ForeignKey('lead.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            description = Column(Text, nullable=False)
            completed = Column(Boolean, nullable=False, default=False)
            date = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))

        class Application(self.Base):
            __tablename__ = 'application'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            advisor_code = Column(Integer, ForeignKey('advisor.code', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            lead_id = Column(UUID(as_uuid=True), ForeignKey('lead.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            master_account = Column(Text, nullable=True)
            date_sent_to_ibkr = Column(Text, nullable=True)
            application = Column(JSONB, nullable=True)
            status = Column(Text, nullable=False, default='Started')
            security_questions = Column(JSONB, nullable=True)
        
        class Account(self.Base):
            __tablename__ = 'account'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            application_id = Column(UUID(as_uuid=True), ForeignKey('application.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            advisor_code = Column(Integer, ForeignKey('advisor.code', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            ibkr_account_number = Column(Text, nullable=False, unique=True)
            ibkr_username = Column(Text, nullable=True)
            ibkr_password = Column(Text, nullable=True)
            temporal_email = Column(Text, nullable=True)
            temporal_password = Column(Text, nullable=True)
            fee_template = Column(Text, nullable=True)
            master_account = Column(Text, nullable=True)
            management_type = Column(Text, nullable=True)

        class FeeTemplateRequest(self.Base):
            __tablename__ = 'fee_template_request'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            requested_by = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            approved_by = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=True)
            replied_time = Column(Text, nullable=True)
            previous_template = Column(Text, nullable=False)
            new_template = Column(Text, nullable=False)
            approved = Column(Boolean, nullable=False, default=False)

        class AdvisorChangeRequest(self.Base):
            __tablename__ = 'advisor_change_request'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            requested_by = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            old_advisor_code = Column(Integer, ForeignKey('advisor.code', ondelete='CASCADE', onupdate='CASCADE'), nullable=True)
            new_advisor_code = Column(Integer, ForeignKey('advisor.code', ondelete='CASCADE', onupdate='CASCADE'), nullable=True)

        class ManagementTypeRequest(self.Base):
            __tablename__ = 'management_type_request'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            requested_by = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            previous_type = Column(Text, nullable=True)
            new_type = Column(Text, nullable=True)

        class AccountDocument(self.Base):
            __tablename__ = 'account_document'
            id = Column(UUID(as_uuid=True), unique=True, primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            document_id = Column(UUID(as_uuid=True), ForeignKey('document.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)

        class Document(self.Base):
            __tablename__ = 'document'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            file_name = Column(Text, nullable=False)
            file_length = Column(Text, nullable=False)
            sha1_checksum = Column(Text, nullable=False)
            mime_type = Column(Text, nullable=False)
            data = Column(Text, nullable=False)

        class RiskProfile(self.Base):
            __tablename__ = 'risk_profile'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=True)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=True)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            risk_profile_id = Column(Integer, nullable=False)
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            score = Column(Text, nullable=False)
            name = Column(Text, nullable=True)
            answers = Column(JSONB, nullable=True)

        class InvestmentProposal(self.Base):
            __tablename__ = 'investment_proposal'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            risk_profile_id = Column(UUID(as_uuid=True), ForeignKey('risk_profile.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            aaa_a = Column(ARRAY(JSONB), nullable=False)
            bbb = Column(ARRAY(JSONB), nullable=False)
            bb = Column(ARRAY(JSONB), nullable=False)
            etfs = Column(ARRAY(JSONB), nullable=False)

        class TradeTicket(self.Base):
            __tablename__ = 'trade_ticket'
            id = Column(Text, primary_key=True)
            user_id = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            name = Column(Text, nullable=False)

        class PendingTask(self.Base):
            __tablename__ = 'pending_task'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False, unique=True)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            description = Column(Text, nullable=False)
            date = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            closed = Column(Text, nullable=True, default=None)
            tags = Column(ARRAY(Text), nullable=True)
            priority = Column(Integer, nullable=False)
            emails_to_notify = Column(ARRAY(Text), nullable=True)

        class PendingTaskFollowUp(self.Base):
            __tablename__ = 'pending_task_follow_up'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            pending_task_id = Column(UUID(as_uuid=True), ForeignKey('pending_task.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            description = Column(Text, nullable=False)
            completed = Column(Boolean, nullable=False, default=False)
            date = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))

        # Contacts
        self.User = User
        self.Contact = Contact
        self.Application = Application
        self.Advisor = Advisor

        # Leads
        self.Lead = Lead
        self.FollowUp = FollowUp

        # Accounts
        self.Account = Account
        self.AccountDocument = AccountDocument
        self.Document = Document
        self.FeeTemplateRequest = FeeTemplateRequest
        self.AdvisorChangeRequest = AdvisorChangeRequest
        self.ManagementTypeRequest = ManagementTypeRequest
        
        # Risk Profiles
        self.RiskProfile = RiskProfile    

        # Investment Proposals
        self.InvestmentProposal = InvestmentProposal

        # Trade Tickets
        self.TradeTicket = TradeTicket

        # Pending Tasks
        self.PendingTask = PendingTask
        self.PendingTaskFollowUp = PendingTaskFollowUp

# Create a single instance that can be imported and used throughout the application
db = Supabase().db