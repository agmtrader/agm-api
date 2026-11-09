from src.components.tools.public.trade_confirmations import get_trade_confirmations_report
from src.utils.connectors.supabase import initialize_database

initialize_database()

print(get_trade_confirmations_report())