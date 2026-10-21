from sqlalchemy import CheckConstraint, Date, Index, BigInteger, Boolean, ForeignKey, Text, create_engine, Column, Integer, BIGINT, Numeric, Identity
from sqlalchemy.engine import URL, make_url
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
import uuid
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import os
from src.utils.managers.database_manager import DatabaseManager
from src.utils.logger import logger
from src.utils.managers.secret_manager import get_secret


def application_database_url(value):
    """Multiplex shared Supabase application connections; preserve other endpoints.

    psycopg2 does not automatically prepare statements. Application operations
    must finish their transactions and cannot depend on session-level state.
    Maintenance tools can explicitly select DB_POOL_MODE=session or preserve.
    """
    mode = os.getenv('DB_POOL_MODE', 'transaction').lower()
    if mode not in {'transaction', 'session', 'preserve'}:
        raise ValueError('DB_POOL_MODE must be transaction, session, or preserve')
    url = make_url(value)
    if mode == 'preserve' or not (url.host or '').endswith('.pooler.supabase.com'):
        return url
    if url.port not in (None, 5432, 6543):
        raise ValueError('Unrecognized shared Supabase pooler port')
    return url.set(port=6543 if mode == 'transaction' else 5432)


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

            self.db_url = application_database_url(self._resolve_db_url())
            self.engine = self._create_engine()
            
            self.Base = declarative_base()
            self._setup_models()
            
            self.db = DatabaseManager(
                base=self.Base,
                engine=self.engine,
                engine_factory=self._create_engine,
            )
            
            logger.announcement('Successfully initialized Database Service', 'success')
            self._initialized = True

    def _resolve_db_url(self):
        env_db_url = os.getenv('SUPABASE_DB_URL')
        if env_db_url:
            logger.info('Using SUPABASE_DB_URL from environment')
            return env_db_url

        try:
            secret_db_url = get_secret('SUPABASE_DB_URL')
            if secret_db_url:
                logger.info('Using SUPABASE_DB_URL from Secret Manager')
                return secret_db_url
        except Exception:
            logger.warning('SUPABASE_DB_URL secret not found. Falling back to transaction pooler credentials.')

        supabase_user = get_secret('SUPABASE_USER')
        supabase_password = get_secret('SUPABASE_PASSWORD')

        return URL.create(
            drivername='postgresql',
            username=f'postgres.{supabase_user}',
            password=supabase_password,
            host='aws-0-us-west-1.pooler.supabase.com',
            port=6543,
            database='postgres',
            query={'gssencmode': 'disable'},
        ).render_as_string(hide_password=False)

    def _create_engine(self):
        default_size = '1' if os.getenv('DEV_MODE', '').lower() == 'true' else '3'
        pool_size = int(os.getenv('DB_POOL_SIZE', default_size))
        max_overflow = int(os.getenv('DB_MAX_OVERFLOW', '0'))
        pool_timeout = int(os.getenv('DB_POOL_TIMEOUT', '15'))
        if pool_size < 1 or max_overflow < 0 or pool_timeout < 1:
            raise ValueError('DB_POOL_SIZE and DB_POOL_TIMEOUT must be positive; DB_MAX_OVERFLOW must be non-negative')

        runtime = os.getenv('K_SERVICE') or os.getenv('CLOUD_RUN_JOB') or 'dev'

        engine = create_engine(
            self.db_url,
            pool_pre_ping=True,
            pool_recycle=300,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_use_lifo=True,
            connect_args={
                'connect_timeout': 10,
                'sslmode': 'require',
                'gssencmode': 'disable',
                'application_name': f'agm:{runtime}:{os.getpid()}'[:63],
            },
        )
        logger.info(
            'Created Supabase engine '
            f'(port={make_url(self.db_url).port}, pool_size={pool_size}, '
            f'max_overflow={max_overflow}, pool_timeout={pool_timeout})'
        )
        return engine

    def _setup_models(self):

        class User(self.Base):
            __tablename__ = 'user'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            email = Column(Text, nullable=True, unique=True)
            password_hash = Column(Text, nullable=True)
            scopes = Column(Text, nullable=True)
            last_login = Column(Text, nullable=True)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
        
        class Account(self.Base):
            __tablename__ = 'account'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            advisor_code = Column(Integer, ForeignKey('advisor.code', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            ibkr_account_number = Column(Text, nullable=True, unique=True)
            ibkr_username = Column(Text, nullable=True)
            ibkr_password_secret_id = Column(UUID(as_uuid=True), nullable=True)
            temporal_email = Column(Text, nullable=True)
            temporal_password_secret_id = Column(UUID(as_uuid=True), nullable=True)
            master_account = Column(Text, nullable=True)
            management_type = Column(Text, nullable=True)
            emailed_credentials = Column(Boolean, nullable=False, default=False)
            referrer = Column(Text, nullable=True)
            date_sent_to_ibkr = Column(Text, nullable=True)
            application_json = Column(JSONB, nullable=True)
            estimated_deposit = Column(BIGINT, nullable=True)
            application_provider_id = Column(UUID(as_uuid=True), ForeignKey('application_provider.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)

        class ApplicationProvider(self.Base):
            __tablename__ = 'application_provider'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            color_scheme = Column(JSONB, nullable=False, default={})

        class AccountContact(self.Base):
            __tablename__ = 'account_contact'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=True, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=True, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            entity_id = Column(BIGINT, nullable=True)
            external_id = Column(Text, nullable=True)
            role = Column(Text, nullable=True)

        class AccountComment(self.Base):
            __tablename__ = 'account_comments'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            user_id = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            author_name = Column(Text, nullable=False)
            author_email = Column(Text, nullable=True)
            body = Column(Text, nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            edited_at = Column(Text, nullable=True)
            deleted_at = Column(Text, nullable=True)

        class AccountInstruction(self.Base):
            __tablename__ = 'account_instruction'
            id = Column(BigInteger, primary_key=True, autoincrement=True)
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))

        class ExternalAssetTransfer(self.Base):
            __tablename__ = 'external_asset_transfer'
            id = Column(UUID(as_uuid=True), primary_key=True)
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id'), nullable=False)
            master_account = Column(Text, nullable=False)
            ibkr_account_id = Column(Text, nullable=False)
            client_instruction_id = Column(BigInteger, Identity(start=1000000000000000), nullable=False, unique=True)
            submission_state = Column(Text, nullable=False)
            request_data = Column(JSONB, nullable=False)
            instruction_id = Column(Text, nullable=True)
            instruction_set_id = Column(Text, nullable=True)
            ib_reference_id = Column(Text, nullable=True)
            instruction_status = Column(Text, nullable=True)
            clearing_state = Column(Text, nullable=True)
            last_error = Column(JSONB, nullable=True)
            message_center_ticket_reference = Column(Text, nullable=True)
            ticket_created_at = Column(Text, nullable=True)
            ticket_recorded_at = Column(Text, nullable=True)
            ticket_url = Column(Text, nullable=True)
            portal_transfer_reference = Column(Text, nullable=True)
            receiving_broker_bmo_acceptance = Column(Boolean, nullable=True)
            created = Column(Text, nullable=False)
            updated = Column(Text, nullable=False)

        class Advisor(self.Base):
            __tablename__ = 'advisor'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            code = Column(Integer, nullable=False, unique=True, server_default='SELECT MAX(1, MAX(code)) FROM advisor')
            agency = Column(Text, nullable=False)
            hierarchy1 = Column(Text, nullable=False)
            hierarchy2 = Column(Text, nullable=False)
            name = Column(Text, nullable=False)

        class AdvisorChangeRequest(self.Base):
            __tablename__ = 'advisor_change_request'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            requested_by = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            old_advisor_code = Column(Integer, ForeignKey('advisor.code', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            new_advisor_code = Column(Integer, ForeignKey('advisor.code', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)

        class Contact(self.Base):
            __tablename__ = 'contact'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            type = Column(Text, nullable=True)
            name = Column(Text, nullable=False)
            phone = Column(Text, nullable=True)
            email = Column(Text, nullable=True)
            met_place = Column(Text, nullable=True)

        class ContactIdentity(self.Base):
            __tablename__ = 'contact_identity'
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True)
            date_of_birth = Column(Date, nullable=False)
            source_account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            source = Column(Text, nullable=False)
            verification_status = Column(Text, nullable=False)
            created = Column(Text, nullable=False)
            updated = Column(Text, nullable=False)
            __table_args__ = (
                CheckConstraint("source = 'application'"),
                CheckConstraint("verification_status = 'applicant_declared'"),
                Index('contact_identity_source_account_idx', 'source_account_id'),
            )

        class ContactIdentityDocument(self.Base):
            __tablename__ = 'contact_identity_document'
            id = Column(UUID(as_uuid=True), primary_key=True)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            document_type = Column(Text, nullable=False)
            issuing_country = Column(Text, nullable=False)
            identifier = Column(Text, nullable=False)
            source_account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            source = Column(Text, nullable=False)
            verification_status = Column(Text, nullable=False)
            created = Column(Text, nullable=False)
            updated = Column(Text, nullable=False)
            __table_args__ = (
                CheckConstraint("document_type in ('passport','nationalCard','driversLicense')"),
                CheckConstraint("issuing_country ~ '^[A-Z]{3}$'"),
                CheckConstraint("identifier ~ '^[A-Z0-9]{1,128}$'"),
                CheckConstraint("source = 'application'"),
                CheckConstraint("verification_status = 'applicant_declared'"),
                Index('contact_identity_document_scoped_id_idx', 'document_type', 'issuing_country', 'identifier', unique=True),
                Index('contact_identity_document_contact_idx', 'contact_id'),
                Index('contact_identity_document_source_account_idx', 'source_account_id'),
            )

        class ContactDocument(self.Base):
            __tablename__ = 'contact_document'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            document_id = Column(UUID(as_uuid=True), ForeignKey('document.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            category = Column(Text, nullable=True)
            type = Column(Text, nullable=True)
            document_language = Column(Text, nullable=True)
            issued_date = Column(Text, nullable=True)
            expiry_date = Column(Text, nullable=True)
            comment = Column(Text, nullable=True)

        class ContactScreening(self.Base):
            __tablename__ = 'contact_screening'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            risk_score = Column(Text, nullable=True)
            fatf_status = Column(JSONB(none_as_null=True), nullable=True)
            un_status = Column(JSONB(none_as_null=True), nullable=True)
            uk_status = Column(JSONB(none_as_null=True), nullable=True)
            ofac_results = Column(JSONB(none_as_null=True), nullable=True)

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

        class DocumentProcessing(self.Base):
            __tablename__ = 'document_processing'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            document_id = Column(UUID(as_uuid=True), ForeignKey('document.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            process_type = Column(Text, nullable=False)
            status = Column(Text, nullable=False)
            source_language = Column(Text, nullable=True)
            output_text = Column(Text, nullable=True)
            provider = Column(Text, nullable=True)
            error = Column(Text, nullable=True)

        class DocumentReviewResponsible(self.Base):
            __tablename__ = 'document_review_responsible'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False, unique=False)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False, unique=False)
            user_id = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=True, unique=False)
            comment = Column(Text, nullable=True)

        class DocumentReviewEmail(self.Base):
            __tablename__ = 'document_review_email'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
            recipient_email = Column(Text, nullable=False)
            recipient_source = Column(Text, nullable=False)
            missing_document_keys = Column(JSONB(none_as_null=True), nullable=False, default=list)
            language = Column(Text, nullable=False)
            status = Column(Text, nullable=False, default='pending')
            provider = Column(Text, nullable=False, default='gmail')
            provider_message_id = Column(Text, nullable=True)
            sent_at = Column(Text, nullable=True)
            error_message = Column(Text, nullable=True)

        class FeeTemplateRequest(self.Base):
            __tablename__ = 'fee_template_request'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            requested_by = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            approved_by = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            replied_time = Column(Text, nullable=True)
            previous_template = Column(Text, nullable=False)
            new_template = Column(Text, nullable=False)
            approved = Column(Boolean, nullable=False, default=False)

        class FlaggedDeposit(self.Base):
            __tablename__ = 'flagged_deposit'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            transaction_id = Column(Text, nullable=False)
            comment = Column(Text, nullable=False)
        
        class InvestmentProposal(self.Base):
            __tablename__ = 'investment_proposal'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            # Optional owner for both contact-scoped and global custom proposals.
            # Risk-profile proposals should carry the same contact_id as their
            # linked risk profile; this invariant is enforced by the service layer.
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            risk_profile_id = Column(UUID(as_uuid=True), ForeignKey('risk_profile.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            source_type = Column(Text, nullable=False)
            starting_amount = Column(Numeric(precision=18, scale=2), nullable=True)
            assets = Column(JSONB, nullable=True)

        class ManagementTypeRequest(self.Base):
            __tablename__ = 'management_type_request'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            requested_by = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            previous_type = Column(Text, nullable=True)
            new_type = Column(Text, nullable=True)

        class RiskProfile(self.Base):
            __tablename__ = 'risk_profile'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            contact_id = Column(UUID(as_uuid=True), ForeignKey('contact.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            score = Column(Text, nullable=False)
            name = Column(Text, nullable=True)
            answers = Column(JSONB, nullable=True)
            raw_answers = Column(JSONB, nullable=True)

        class TradeTicket(self.Base):
            __tablename__ = 'trade_ticket'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            user_id = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            name = Column(Text, nullable=False)
            query_id = Column(Text, nullable=False)

        class TransactionMonitoringAlert(self.Base):
            __tablename__ = 'transaction_monitoring_alert'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            transaction_id = Column(Text, nullable=False, unique=True)
            transaction_datetime = Column(Text, nullable=True)
            amount = Column(BIGINT, nullable=True)
            currency = Column(Text, nullable=True)
            direction = Column(Text, nullable=True)
            comment = Column(Text, nullable=True)
            status = Column(Text, nullable=False)
            disposition = Column(Text, nullable=True)
            reviewed_by = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            reviewed_at = Column(Text, nullable=True)
            investigation_id = Column(UUID(as_uuid=True), nullable=True)

        class SuspiciousActivityInvestigation(self.Base):
            __tablename__ = 'suspicious_activity_investigation'
            id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
            created = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            updated = Column(Text, nullable=False, default=datetime.now().strftime('%Y%m%d%H%M%S'))
            account_id = Column(UUID(as_uuid=True), ForeignKey('account.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=False)
            transaction_monitoring_alert_id = Column(UUID(as_uuid=True), ForeignKey('transaction_monitoring_alert.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False, unique=True)
            status = Column(Text, nullable=False)
            risk_level = Column(Text, nullable=True)
            decision = Column(Text, nullable=True)
            decision_rationale = Column(Text, nullable=True)
            decision_by = Column(UUID(as_uuid=True), ForeignKey('user.id', ondelete='SET NULL', onupdate='CASCADE'), nullable=True)
            decision_at = Column(Text, nullable=True)
            closure_date = Column(Text, nullable=True)
            closure_reason = Column(Text, nullable=True)

        self.Account = Account
        self.AccountContact = AccountContact
        self.ApplicationProvider = ApplicationProvider
        self.AccountInstruction = AccountInstruction
        self.AccountComment = AccountComment
        self.Advisor = Advisor
        self.AdvisorChangeRequest = AdvisorChangeRequest
        self.Contact = Contact
        self.ContactIdentity = ContactIdentity
        self.ContactIdentityDocument = ContactIdentityDocument
        self.ContactScreening = ContactScreening
        self.ContactDocument = ContactDocument
        self.Document = Document
        self.DocumentProcessing = DocumentProcessing
        self.DocumentReviewEmail = DocumentReviewEmail
        self.DocumentReviewResponsible = DocumentReviewResponsible
        self.FeeTemplateRequest = FeeTemplateRequest
        self.FlaggedDeposit = FlaggedDeposit
        self.InvestmentProposal = InvestmentProposal
        self.ManagementTypeRequest = ManagementTypeRequest
        self.RiskProfile = RiskProfile    
        self.TradeTicket = TradeTicket
        self.TransactionMonitoringAlert = TransactionMonitoringAlert
        self.SuspiciousActivityInvestigation = SuspiciousActivityInvestigation
        self.User = User

class DatabaseProxy:
    """Stable reference to the explicitly initialized database manager.

    Components can continue importing ``db`` without causing database
    connection or schema validation during module import. The application
    startup path must call :func:`initialize_database` first.
    """

    def __init__(self):
        self._target = None

    def set_target(self, target):
        self._target = target

    def __getattr__(self, name):
        if self._target is None:
            raise RuntimeError(
                'Database has not been initialized. '
                'Call initialize_database() during application startup.'
            )
        return getattr(self._target, name)


_supabase_instance = None
db = DatabaseProxy()


def initialize_database():
    """Initialize declared runtime metadata once; validation is deployment work."""
    global _supabase_instance

    if _supabase_instance is None:
        _supabase_instance = Supabase()
        db.set_target(_supabase_instance.db)

    return db
