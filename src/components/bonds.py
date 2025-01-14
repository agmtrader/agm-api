from ib_insync import *
import pandas as pd
from src.utils.logger import logger
from datetime import datetime
from src.utils.api import access_api
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from src.utils.database import DatabaseHandler
from src.utils.response import Response
from sqlalchemy import create_engine
import os
import asyncio
import math
import nest_asyncio

nest_asyncio.apply()

db_path = os.path.join(os.path.dirname(__file__), '..', 'db', 'bonds.db')
db_url = f'sqlite:///{db_path}'

engine = create_engine(db_url)
Base = declarative_base()

class AGMBond(Base):
    """Expense table"""
    __tablename__ = 'bond'
    id = Column(Integer, primary_key=True, autoincrement=True)
    financial_instrument = Column(String, nullable=False)
    company_name = Column(String, nullable=False)
    avg_price = Column(Float, nullable=False)
    ticker_action = Column(String, nullable=False)
    position = Column(Float, nullable=False)
    bid_size = Column(Float, nullable=False)
    bid = Column(Float, nullable=False)
    bid_yield = Column(Float, nullable=False)
    ask_size = Column(Float, nullable=False)
    ask = Column(Float, nullable=False)
    ask_yield = Column(Float, nullable=False)
    daily_pnl = Column(Float, nullable=False)
    duration_percent = Column(Float, nullable=False)
    current_yield = Column(Float, nullable=False)
    sector = Column(String, nullable=False)
    industry = Column(String, nullable=False)
    maturity = Column(String, nullable=False)
    next_option_date = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    coupon = Column(Float, nullable=False)
    change = Column(Float, nullable=False)
    change_percent = Column(Float, nullable=False)
    last = Column(Float, nullable=False)
    ratings = Column(String, nullable=False)
    payment_frequency = Column(String, nullable=False)
    issuer_country = Column(String, nullable=False)
    trading_currency = Column(String, nullable=False)
    amount_outstanding = Column(Float, nullable=False)
    exchange_listed = Column(String, nullable=False)
    issuer_equity_market_cap = Column(Float, nullable=False)
    issuer_debt_outstanding = Column(Float, nullable=False)
    issuer_book_value = Column(Float, nullable=False)
    issuer_tangible_book_value = Column(Float, nullable=False)
    issuer_debt_book = Column(Float, nullable=False)
    issuer_debt_equity = Column(Float, nullable=False)
    issuer_equity_book = Column(Float, nullable=False)
    issuer_equity_tangible_book = Column(Float, nullable=False)
    issuer_debt_tangible_book = Column(Float, nullable=False)
    last_yield = Column(Float, nullable=False)
    mark_yield = Column(Float, nullable=False)
    bond_attributes = Column(String, nullable=False)
    convexity = Column(Float, nullable=False)
    value_of_basis_point = Column(Float, nullable=False)
    cusip = Column(String, nullable=False)
    issue_date = Column(String, nullable=False)
    last_trading_date = Column(String, nullable=False)
    face_value = Column(Float, nullable=False)
    moodys = Column(String, nullable=False)
    sp = Column(String, nullable=False)
    bond_features = Column(String, nullable=False)
    time_to_maturity = Column(Float, nullable=False)
    updated = Column(String, nullable=False)
    created = Column(String, nullable=False)

db = DatabaseHandler(Base, engine)

class Bonds():

    def __init__(self):
        self.ib = IB()
        
    async def connect(self):
        try:
            await self.ib.connectAsync('127.0.0.1', 4001, clientId=1)
            if not self.ib.isConnected():
                logger.error('Failed to connect to IBKR')
                return False
            logger.announcement(f"Connected to IBKR.", 'success')
            return True
        except Exception as e:
            logger.error(f'Error connecting to IBKR: {str(e)}')
            return False
        
    def get(self):

        # Download Open Positions file
        db.delete_all('bond')

        logger.announcement('Downloading Open Positions file')

        response = access_api('/drive/download_file', 'POST', {
            'file_id': '1WRONE8UxwgDphuL_ik3MnX_DejmnDjW0',
            'parse': True
        })
        logger.announcement('Downloading Open Positions file done', 'success')

        open_positions = pd.DataFrame(response['content'])
        bonds_list = open_positions[open_positions['AssetClass'] == 'BOND']
        unique_bonds_list = bonds_list['Conid'].unique()
        logger.announcement(f'{len(unique_bonds_list)} unique bonds found', 'success')

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Connect to IB
            if not loop.run_until_complete(self.connect()):
                return Response.error('Failed to connect to IBKR')

            for conid in unique_bonds_list[:5]:
                try:
                    logger.info(f'Fetching data for bond {conid}')
                    bond = Bond(conId=conid)
                    
                    details = self.ib.reqContractDetails(bond)
                    if not details:
                        logger.warning(f'No details found')
                        continue

                    logger.info(details[0].contract.secId)
                        
                    detail = details[0]
                    
                    self.ib.qualifyContracts(bond)
                    ticker = self.ib.reqMktData(bond, '', False, False)
                    
                    logger.info(f'Waiting for market data to arrive and stabilize')
                    for _ in range(5):
                        self.ib.sleep(1)
                        if hasattr(ticker, 'last') and ticker.last is not None:
                            break

                    # Compile bond information matching desired columns
                    logger.info(f'Compiling bond information')
                    bond_info = {}

                    bond_info['financial_instrument'] = detail.descAppend
                    if detail.contract.exchange is not None:
                        bond_info['exchange_listed'] = 'Yes'
                    else:
                        bond_info['exchange_listed'] = 'No'

                    # IDs
                    bond_info['cusip'] = detail.cusip
                    
                    # Extract ticker data)
                    logger.info(ticker)
                    ask = float(ticker.ask) if ticker.ask and not math.isnan(float(ticker.ask)) else 0
                    ask_size = float(ticker.askSize) if ticker.askSize and not math.isnan(float(ticker.askSize)) else 0
                    ask_yield = float(ticker.askYield) if ticker.askYield and not math.isnan(float(ticker.askYield)) else 0
                    bid = float(ticker.bid) if ticker.bid and not math.isnan(float(ticker.bid)) else 0
                    bid_size = float(ticker.bidSize) if ticker.bidSize and not math.isnan(float(ticker.bidSize)) else 0
                    bid_yield = float(ticker.bidYield) if ticker.bidYield and not math.isnan(float(ticker.bidYield)) else 0
                    last = float(ticker.last) if ticker.last and not math.isnan(float(ticker.last)) else 0
                    last_yield = float(ticker.lastYield) if ticker.lastYield and not math.isnan(float(ticker.lastYield)) else 0

                    # Ticker
                    bond_info['last'] = last
                    bond_info['last_yield'] = last_yield
                    
                    bond_info['bid_size'] = bid_size
                    bond_info['bid'] = bid
                    bond_info['bid_yield'] = bid_yield

                    bond_info['ask_size'] = ask_size
                    bond_info['ask'] = ask
                    bond_info['ask_yield'] = ask_yield

                    # Bond details
                    bond_info['coupon'] = float(detail.coupon)
                    bond_info['industry'] = detail.industry
                    bond_info['issue_date'] = detail.issueDate
                    bond_info['symbol'] = ''
                    bond_info['ratings'] = detail.ratings
                    bond_info['maturity'] = detail.maturity

                    bond_info['next_option_date'] = detail.nextOptionDate
                    bond_info['trading_currency'] = detail.contract.currency
                    bond_info['company_name'] = detail.longName

                    # Misc
                    bond_info['current_yield'] = float(detail.coupon * 100 / ticker.last) if ticker.last and detail.coupon else 0

                    bond_info['avg_price'] = 0
                    bond_info['position'] = 0
                    bond_info['daily_pnl'] = 0
                    bond_info['duration_percent'] = 0
                    bond_info['coupon'] = detail.coupon
                    bond_info['change'] = 0
                    bond_info['change_percent'] = 0
                    bond_info['face_value'] = 0
                    bond_info['mark_yield'] = 0
                    bond_info['convexity'] = 0
                    bond_info['value_of_basis_point'] = 0
                    bond_info['time_to_maturity'] = 0
                    bond_info['issuer_equity_market_cap'] = 0
                    bond_info['issuer_debt_outstanding'] = 0
                    bond_info['issuer_book_value'] = 0
                    bond_info['issuer_tangible_book_value'] = 0
                    bond_info['issuer_debt_book'] = 0
                    bond_info['issuer_debt_equity'] = 0
                    bond_info['issuer_equity_book'] = 0
                    bond_info['issuer_equity_tangible_book'] = 0
                    bond_info['issuer_debt_tangible_book'] = 0
                    bond_info['amount_outstanding'] = 0

                    bond_info['sector'] = ''
                    bond_info['industry'] = ''
                    bond_info['ticker_action'] = ''
                    bond_info['payment_frequency'] = ''
                    bond_info['issuer_country'] = ''
                    bond_info['exchange_listed'] = ''
                    bond_info['bond_attributes'] = ''
                    bond_info['bond_features'] = ''
                    bond_info['last_trading_date'] = ''
                    bond_info['moodys'] = ''
                    bond_info['sp'] = ''
                    
                    db.create('bond', bond_info)
                    logger.info(f'Successfully fetched data')
                    
                except Exception as e:
                    logger.error(f'Error fetching data: {str(e)}')
                    return Response.error(f'Error fetching data: {str(e)}')

            return {'status': 'success', 'message': 'Bonds data fetched successfully'}

        except Exception as e:
            logger.error(f'Error in get method: {str(e)}')
            return {'error': str(e)}
        finally:
            if self.ib.isConnected():
                self.ib.disconnect()
            loop.close()
