from src.utils.connectors.supabase import db
from src.utils.exception import handle_exception
from src.utils.logger import logger

logger.announcement('Initializing Portfolio Plan Service', type='info')
logger.announcement('Initialized Portfolio Plan Service', type='success')


@handle_exception
def create_portfolio_plan(portfolio_plan: dict = None):
    if portfolio_plan is None:
        raise Exception('portfolio_plan payload is required')

    portfolio_plan_id = db.create(table='portfolio_plan', data=portfolio_plan)
    return {'id': portfolio_plan_id}


@handle_exception
def read_portfolio_plans(query: dict = None):
    return db.read(table='portfolio_plan', query=query or {})


@handle_exception
def update_portfolio_plan(query: dict = None, portfolio_plan: dict = None):
    if query is None:
        raise Exception('query payload is required')
    if portfolio_plan is None:
        raise Exception('portfolio_plan payload is required')

    portfolio_plan_id = db.update(table='portfolio_plan', query=query, data=portfolio_plan)
    return {'id': portfolio_plan_id}
