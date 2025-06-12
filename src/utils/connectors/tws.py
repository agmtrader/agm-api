from ib_insync import *
from src.utils.logger import logger
import time
import math
import os

logger.announcement("Initializing TWS Connector", 'info')
logger.announcement("TWS Connector initialized", 'success')

class TWS:

    def __init__(self):
        self.ib = IB()
        self.account_summary = None
        self.host = os.getenv('IBKR_HOST', None)
        self.port = int(os.getenv('IBKR_PORT', None))
        self.connect()

    def connect(self):
        logger.info(f"Connecting to IBKR at {self.host}:{self.port}")
        try:
            if self.host is None or self.port is None:
                raise Exception("IBKR_HOST and IBKR_PORT must be set in the environment variables.")
            
            self.ib.connect(self.host, self.port, clientId=1)
            if self.ib.isConnected():
                logger.success("Connected to IBKR")
                return True
            else:
                logger.error("Failed to connect to IBKR")
                return False
        except Exception as e:
            logger.error(f"Error connecting to IB: {str(e)}")
            raise Exception(f"Error connecting to IB: {str(e)}")
        
    def disconnect(self):
        logger.info("Disconnecting from IBKR")
        if self.ib.isConnected():
            try:
                self.ib.disconnect()
                logger.success("Disconnected from IBKR")
                return True
            except Exception as e:
                logger.error(f"Error disconnecting from IB: {str(e)}")
                return False
        return False

    def get_historical_data(self, contract: Contract):
        logger.info(f"Getting historical data")
        try:
            historical_data_response = self.ib.reqHistoricalData(
                contract, 
                endDateTime='', 
                durationStr='1 Y', 
                barSizeSetting='1 day', 
                whatToShow='TRADES', 
                useRTH=1
            )
            historical_data = []
            for bar in historical_data_response:
                historical_data.append(bar.dict())
            
            logger.success(f"Successfully got historical data")
            return historical_data
        except Exception as e:
            logger.error(f"Error getting historical data: {str(e)}")
            raise Exception(f"Error getting historical data: {str(e)}")

    def get_latest_price(self, contract: Contract):
        logger.info(f"Getting latest price")
        try:
            self.ib.reqMarketDataType(3)
            market_data_response = self.ib.reqMktData(contract, '233', False, False, [])
            
            # Wait for market data
            timeout = 10  # 10 second timeout
            start_time = time.time()
            while math.isnan(market_data_response.last) and (time.time() - start_time) < timeout:
                self.ib.sleep(0.1)
                logger.info(f"Waiting for market data...")
            
            if math.isnan(market_data_response.last):
                raise Exception("Timeout waiting for market data")
                
            latest_price = market_data_response.last
            logger.success(f"Successfully got latest price: {latest_price}")
            return latest_price
        except Exception as e:
            logger.error(f"Error getting latest price: {str(e)}")
            raise Exception(f"Error getting latest price: {str(e)}")

    def get_account_summary(self):
        logger.info("Getting account summary")
        try:
            account_summary_response = self.ib.accountSummary()
            account_summary = []
            for summary in account_summary_response:
                account_summary_dict = {}
                account_summary_dict['account'] = summary.account
                account_summary_dict['tag'] = summary.tag
                account_summary_dict['value'] = summary.value
                account_summary_dict['currency'] = summary.currency
                account_summary_dict['modelCode'] = summary.modelCode
                account_summary.append(account_summary_dict)
            
            logger.success(f"Successfully got account summary")
            return account_summary
        except Exception as e:
            logger.error(f"Error getting account summary: {str(e)}")
            raise Exception(f"Error getting account summary: {str(e)}")

    def get_positions(self):
        logger.info("Getting positions")
        try:
            positions = self.ib.positions()
            logger.info(f"You have {len(positions)} positions overall")
            
            # Convert positions to serializable format
            positions_data = []
            for pos in positions:
                positions_data.append({
                    'contract': pos.contract.dict(),
                    'position': pos.position,
                    'avgCost': pos.avgCost
                })
            
            logger.success(f"Successfully got positions")
            return positions_data
        except Exception as e:
            logger.error(f"Error getting positions: {str(e)}")
            raise Exception(f"Error getting positions: {str(e)}")

    def get_completed_orders(self):
        logger.info("Getting completed orders")
        try:
            orders_response = self.ib.reqCompletedOrders(False)
            orders = []
            for order in orders_response:
                orders.append({
                    'contract': order.contract.dict(),
                    'orderStatus': order.orderStatus.dict(),
                    'isActive': order.isActive(),
                    'isDone': order.isDone(),
                    'filled': order.filled(),
                    'remaining': order.remaining(),
                })
            logger.info(f"Successfully got {len(orders)} completed orders")
            logger.success(f"Successfully got {len(orders)} completed orders")
            return orders
        except Exception as e:
            logger.error(f"Error getting completed orders: {str(e)}")
            raise Exception(f"Error getting completed orders: {str(e)}")

    def get_open_orders(self):
        logger.info("Getting open orders")
        try:
            orders_response = self.ib.openOrders()
            orders = []
            for order in orders_response:
                order_dict = order.dict()
                order_dict['softDollarTier'] = order.softDollarTier.dict()
                orders.append(order_dict)
            logger.info(f"Successfully got {len(orders)} open orders")
            logger.success(f"Successfully got {len(orders)} open orders")
            return orders
        except Exception as e:
            logger.error(f"Error getting open orders: {str(e)}")
            raise Exception(f"Error getting open orders: {str(e)}")

    def place_order(self, contract: Contract, order: Order):
        logger.info(f"Placing order: {order}")
        try:
            self.ib.qualifyContracts(contract)
            trade = self.ib.placeOrder(contract, order)
            logger.success(f"Successfully placed order")
            return {
                'orderId': trade.order.orderId,
                'status': trade.orderStatus.status,
                'contract': trade.contract.dict()
            }
        except Exception as e:
            logger.error(f"Error placing order: {str(e)}")
            raise Exception(f"Error placing order: {str(e)}")
        
    def close_all_positions(self):
        logger.info("Closing all positions")
        try:
            orders = self.ib.orders()
            for order in orders:
                self.ib.cancelOrder(order)
            logger.success("Successfully closed all positions")
            return True
        except Exception as e:
            logger.error(f"Error closing all positions: {str(e)}")
            raise Exception(f"Error closing all positions: {str(e)}")