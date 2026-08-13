from src.utils.connectors.supabase import initialize_database
from src.components.tools.private.actions import update_account_aliases

initialize_database()
update_account_aliases()
