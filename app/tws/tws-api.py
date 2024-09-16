from ibapi.client import *
from ibapi.wrapper import * 
from ibapi.contract import Contract
from ibapi.order import Order

from termcolor import colored

import threading
import time

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
            
    def tickPrice(self, reqId: int, tickType: int, price: float, attrib: TickAttrib):
        dataType = TickTypeEnum.to_str(tickType)
        if dataType == 'MARK_PRICE':
            print('Latest price:', {'latestPrice':price})

    """Executions"""

    def orderStatus(self, orderId: OrderId, status: str, filled: Decimal, remaining: Decimal, avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)

    def completedOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
        print('Completed order:', {orderId, contract, order, orderState})

    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        execution = {'conid':contract.conId, "symbol": contract.symbol, "type":contract.secType, "currency": contract.currency, 'account': execution.acctNumber, 'time':execution.time, 'side':execution.side, 'avgPrice':execution.avgPrice}
        print('Execution:', execution)
        # TODO If execution belongs to self.currentAccount AND
        # TODO If execution is not in params['executions'], push to array

    def commissionReport(self, commissionReport: CommissionReport):
        print('Commission:', {"commission": commissionReport})

    def execDetailsEnd(self, reqId: int):
        print("No more executions.", {'requestId':reqId})
        pass

class AGMAgent:

    def __init__(self):

        self.orderId = 1
        self.currentAccount = "U7906488"

        self.params = {'executions':[]}

        # Initialize AGM Agent
        print(colored(f'Using account {self.currentAccount}', "blue"))

        # Intialize an IBKR API connection
        print(colored('Initializing connection to Interactive Brokers...\n', "white"))
        time.sleep(1)

        # Initialize variables
        paper = False

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
        print(colored('Connection successful. Requesting data from IBKR...\n', "white"))
        time.sleep(1)

        # Subscribe to data
        self.getExecutionUpdates()
        time.sleep(1)

        # Request positions
        #self.app.reqPositions()
        time.sleep(1)

        print(colored(f'Fetching successful. Starting TWS API', "light_blue"))
        print('\n')

        time.sleep(1)
        while True:
            # TODO Create a function to push to database using API sending params['executions'] to db/temp/executions
            print(colored(f"Ping.", "red", attrs=["bold"]))
            time.sleep(500)
    
    def getMarketData(self, ticker):

        contract = self.createContract(ticker, 'STK')
        self.app.reqMktData(ticker, contract, "106,232,221,587,165,258", False, False, [])
        time.sleep(1)
 
    def getExecutionUpdates(self):
        
        self.app.reqAutoOpenOrders(True)
        execution = ExecutionFilter()
        execution.acctCode = self.currentAccount
        self.app.reqExecutions(self.orderId, execution)
        time.sleep(1)

    def createContract(self, ticker, secType, strike=None, right=None, date=None):

        contract = Contract()
        contract.symbol = ticker
        contract.secType = secType
        contract.exchange = "SMART"
        contract.currency = "USD"

        return contract

AGM = AGM()
Agent = AGMAgent()