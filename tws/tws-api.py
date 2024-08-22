from ibapi.client import *
from ibapi.wrapper import * 
from ibapi.contract import Contract
from ibapi.order import Order

from termcolor import colored

from datetime import datetime

import threading
import time

import yfinance as yf

import pandas as pd

from dictionaries import Tickers, MarketDataType

class TWSApp(EWrapper, EClient):

    def __init__(self, AutoTrader):
        EClient.__init__(self, self)
        self.trader = AutoTrader
        self.currentAccount = self.trader.currentAccount

    def nextValidId(self, orderId: int):
        self.trader.orderId = orderId

    def position(self, account: str, contract: Contract, position: float, avgCost: float):
        print('Position:', {'position':position})

    def positionEnd(self):
        print("No more positions.")
        pass

    """Price Data"""
    def tickGeneric(self, reqId: int, tickType: int, value: float):
        dataType = TickTypeEnum.to_str(tickType)
        #print('Implied volatility:', {'ticker': Tickers(reqId).name, "impVolatilty": value})
            
    def tickPrice(self, reqId: int, tickType: int, price: float, attrib: TickAttrib):
        dataType = TickTypeEnum.to_str(tickType)
        if dataType == 'MARK_PRICE':
            print('Latest price:', {'latestPrice':price})

    def tickOptionComputation(self, reqId: int, tickType: int, tickAttrib: int, impliedVol: float, delta: float, optPrice: float, pvDividend: float, gamma: float, vega: float, theta: float, undPrice: float):
        dataType = TickTypeEnum.to_str(tickType)
        print('Option data:', {'type':dataType, 'price':optPrice, 'underlying_px':undPrice,'greeks':{'delta':delta, 'gamma':gamma, 'vega':vega, 'theta':theta}, 'impliedVol':impliedVol})

    def securityDefinitionOptionParameter(self, reqId: int, exchange: str, underlyingConId: int, tradingClass: str, multiplier: str, expirations: SetOfString, strikes: SetOfFloat):
        print('Exchange option data:', {'reqId': reqId, "exchange":exchange, "underlyingConId": underlyingConId, "tradingclass": tradingClass, "multiplier": multiplier, "expirations":expirations, 'strikes':strikes})
    
    """Executions"""
    def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
        print('Open order:', {'orderId':orderId, 'ticker':contract.symbol, 'type':order.orderType,  'orderState':orderState.status})

    def openOrderEnd(self):
        print("No more open orders")

    def orderBound(self, orderId: int, apiClientId: int, apiOrderId: int):
        print("Order Bound:.", "OrderId:", intMaxString(orderId), "ApiClientId:", intMaxString(apiClientId), "ApiOrderId:", intMaxString(apiOrderId))

    def orderStatus(self, orderId: OrderId, status: str, filled: Decimal, remaining: Decimal, avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)

    def completedOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
        print('Completed order:', {orderId, contract, order, orderState})

    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        print('Execution:', {'conid':contract.conId, "symbol": contract.symbol, "type":contract.secType, "currency": contract.currency, 'account': execution.acctNumber, 'time':execution.time, 'side':execution.side, 'avgPrice':execution.avgPrice})

    def commissionReport(self, commissionReport: CommissionReport):
        print('Commission:', {"commission": commissionReport})

    def execDetailsEnd(self, reqId: int):
        print("No more executions.", {'requestId':reqId})
        pass

class AGMAgent:

    def __init__(self):

        self.orderId = 1
        self.currentAccount = "DU8492179"

        # Initialize Auto Trader
        print(colored(f'Using account {self.currentAccount}', "blue"))

        # Intialize an IBKR API connection
        print(colored('Initializing connection to Interactive Brokers...\n', "white"))
        time.sleep(1)

        # Initialize variables
        paper = True
        self.params = {}

        self.app = TWSApp(self)
        self.app.connect('127.0.0.1', 7497 if paper else 7496, clientId=0)

        # Start a websocket to the app
        def websocket_con():
            self.app.run()

        # Enable threading on the websocket for commands
        con_thread = threading.Thread(target=websocket_con, daemon=True)
        con_thread.start()
        time.sleep(1)

        print('\n')
        print(colored('Connection successful. Building Auto Trader...\n', "white"))
        time.sleep(1)

        # Subscribe to data
        self.getExecutionUpdates()
        time.sleep(1)

        # Request positions
        self.app.reqPositions()
        time.sleep(1)

        # Request market data
        marketDataType = MarketDataType.DELAYED.value
        self.app.reqMarketDataType(marketDataType)
        time.sleep(1)
        for ticker in Tickers:
            self.params[ticker.name] = {}
            self.getMarketData(ticker.name)
            time.sleep(1)

        print('\n')
        print(colored(f'Build successful.', "light_blue"))
        print('\n')

        time.sleep(1)
        while True:
            print(colored(f"Ping.", "red", attrs=["bold"]))
            print('Params', self.params)
            time.sleep(1)
    
    def getMarketData(self, ticker):

        contract = self.createContract(ticker, 'STK')
        self.app.reqMktData(ticker, contract, "106,232,221,587,165,258", False, False, [])
        time.sleep(1)
 
    def getExecutionUpdates(self):
        
        self.app.reqAutoOpenOrders(True)
        time.sleep(1)


    def createContract(self, ticker, secType, strike=None, right=None, date=None):

        contract = Contract()
        contract.symbol = ticker
        contract.secType = secType
        contract.exchange = "SMART"
        contract.currency = "USD"

        return contract

    def createOrder(self, action):
        order = Order()
        order.totalQuantity = 1
        order.action = action
        tif = 'DAY'
        order.orderType = 'MKT'
        return order

Agent = AGMAgent()
TWSApp(Agent)