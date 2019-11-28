from datetime import datetime as dt
import logging
import os
from Core import Settings as settings
import threading
import time

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order


# TODO: 
# generate contract
# send an order
# error handling
# logging

# ! CLIENT                      # Client Cancel             # WRAPPER
# ? IsConnected()               
# ? reqAccountSummary()         cancelAccountSummary        AccountSummary
# ? reqIds()                                                nextValidId
# ? reqMktData()                cancelMktData               tickGeneric???  
# ? reqOpenOrders()                                         openOrder/openStatus
# ? reqPositions()                                          position
# ?                                                         positionEnd()
# ? reqMatchingSymbols                                      symbolSamples
# ? reqPnL                      cancelPnL                   pnl
# ? reqPnLSingle                cancelPnLSingle             pnlSingle
# ?                             reqGlobalCancel
# ?                             cancelOrder
# ? queryDisplayGroups()                                    displayGroupList
# ? reqContractDetails()                                    contractDetails
# ? reqCurrentTime()                                        currentTime
# ? reqScannerSubscription()    cancelScannerSubscription   scannerData
# ? reqAllOpenOrders()                                      position

def printall(func):
    def inner(*args, **kwargs):
        print('Args passed: {}'.format(args))
        print('Kwargs passed: {}'.format(kwargs))
        print(func.__code__.co_varnames)
        return func(*args, **kwargs)
    return inner

# TODO:
# def logall(logger):
#     def outer(func):
#         def inner(*args, **kwargs):
#             logger.info(*args, **kwargs)
#             return func(*args, **kwargs)
#         return inner
#     return outer


def _setup_log(name, file, level=logging.INFO):
    if not os.path.exists(settings.log_folder):
        try:
            print(f"Creating log folder in {settings.log_folder}")
            os.mkdir(settings.log_folder)
        except Exception as e:
            print(f"Failed to create log folder in {settings.log_folder}")
            print(f"An error occured {e}")


    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s")

    handler_console = logging.StreamHandler()
    handler_console.setLevel(level)
    handler_console.setFormatter(formatter)

    handler_file = logging.FileHandler(settings.log_folder + r"/" + file, mode="w")
    handler_file.setLevel(level)
    handler_file.setFormatter(formatter)
    
    logger.addHandler(handler_console)
    logger.addHandler(handler_file)

    logger.info("Started")

class IBContract:

    @staticmethod
    def stock(symbol, secType, exchange, currency, primaryExchange):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        contract.primaryExchange = primaryExchange
        return contract

    @staticmethod
    def EurGbpFx():
        contract = Contract()
        contract.symbol = "EUR"
        contract.secType = "CASH"
        contract.currency = "GBP"
        contract.exchange = "IDEALPRO"
        return contract

    @staticmethod
    def USStockSample():
        contract = Contract()
        contract.symbol = "IBKR"
        contract.secType = "STK"
        contract.currency = "USD"
        #In the API side, NASDAQ is always defined as ISLAND in the exchange field
        contract.exchange = "ISLAND" 
        return contract

class IBOrder:

    @staticmethod
    def MarketOrder(action, quantity):
        """
        Action: BUY or SELL
        Quantity
        """
        order = Order()
        order.action = action.upper()
        order.orderType = "MKT"
        order.totalQuantity = quantity
        return order

    @staticmethod
    def LimitOrder(action:str, quantity:float, limitPrice:float):
        """
        Action: BUY or SELL
        Quantity
        LimitPrice
        """
        # ! [limitorder]
        order = Order()
        order.action = action
        order.orderType = "LMT"
        order.totalQuantity = quantity
        order.lmtPrice = limitPrice
        order.transmit=True
        # ! [limitorder]
        return order

    @staticmethod
    def Stop(action:str, quantity:float, stopPrice:float):
        """
        Action: BUY or SELL
        Quantity
        StopPrice
        """
        # ! [stop]
        order = Order()
        order.action = action
        order.orderType = "STP"
        order.auxPrice = stopPrice
        order.totalQuantity = quantity
        # ! [stop]
        return order
    
class _IBWrapper(EWrapper):
    def __init__(self):
        EWrapper.__init__(self)

    def error(self, reqId, errorCode, errorString):
        print(f"ReqID: {reqId}, Code: {errorCode}, Error: {errorString}")
        self.logger.info(f"ReqID: {reqId}, Code: {errorCode}, Error: {errorString}")

    @printall
    def contractDetails(self, reqId, contractDetails):
        print(f"ReqID: {reqId}, Contract Details: {contractDetails}")
        self.logger.info(f"ReqID: {reqId}, Contract Details: {contractDetails}")

    @printall
    def accountSummary(self, reqId, account, tag, value, currency):
        print(f"ReqID: {reqId}, Account: {account}, Tag: {tag}, Value: {value}, Currency: {currency}")
        self.logger.info(f"ReqID: {reqId}, Account: {account}, Tag: {tag}, Value: {value}, Currency: {currency}")

    @printall
    def accountSummaryEnd(self, reqId: int):
        print(f"AccountSummaryEnd. ReqId: {reqId}")
        self.logger.info(f"AccountSummaryEnd. ReqId: {reqId}")

    @printall
    def position(self, account, contract, pos, avg_cost):
        print(f"Account: {account}, Contract: {contract}, Position: {pos}, Average cost: {avg_cost}")
        self.logger.info(f"Account: {account}, Contract: {contract}, Position: {pos}, Average cost: {avg_cost}")

    @printall
    def connectAck(self):
        # print("connectAck CALLED")
        if self.asynchronous:
            self.startApi()

    def openOrder(self, orderId, contract, order, orderState):
        print(f"Order Id: {orderId}, Contract: {contract}, Order: {order}, Order state: {orderState}")
        self.logger.info(f"Order Id: {orderId}, Contract: {contract}, Order: {order}, Order state: {orderState}")   

    def openOrderEnd(self):
        print("Finished executing reqOpenOrders")
        self.logger.info("Finished executing reqOpenOrders")

    @printall
    def nextValidId(self, orderId):
        """
        The nextValidId event provides the next valid identifier needed to place an order. 
        This identifier is nothing more than the next number in the sequence. 
        This means that if there is a single client application submitting orders to an account, 
        it does not have to obtain a new valid identifier every time it needs to submit a new order. 
        It is enough to increase the last value received from the nextValidId method by one.

        However if there are multiple client applications connected to one account, 
        it is necessary to use an order ID with new orders which is greater than all previous 
        order IDs returned to the client application in openOrder or orderStatus callbacks.

        More info http://interactivebrokers.github.io/tws-api/order_submission.html
        """
        super().nextValidId(orderId)
        # logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId
        # print("nextValidId CALLED")
        # print(self.nextValidOrderId)

        print(f"NextValidId: {orderId}")
        self.logger.info(f"NextValidId: {orderId}")


class _IBClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)

class IBApp(_IBWrapper, _IBClient):
    def __init__(self):
        _IBWrapper.__init__(self)
        _IBClient.__init__(self, self)
        
        self.started = False
        self.nextValidOrderId = None
        self.logger = None       

    def start(self):
        if self.started:
            return

        self.started = True
        _setup_log("IBApp", "test.log")
        self.logger = logging.getLogger("IBApp")

        #self.reqIds(-1) # to make sure nextValidOrderId gets a value for sure
 
        # self.reqAllOpenOrders()
        # self.reqCurrentTime()
        
        ib_thread = threading.Thread(target=self.run, name="Interactive Broker Client Thread", )
        ib_thread.start()
        print("Waiting 1 second for nextValidID response")
        self.logger.info("Waiting 1 second for nextValidID response")
        time.sleep(1) 
           
    def nextOrderId(self):
        if self.nextValidOrderId == None:
            self.reqIds(-1)
            time.sleep(1)
            self.nextOrderId()
        else:
            oid = self.nextValidOrderId
            # print("nextOrderId CALLED")
            print(self.nextValidOrderId)
            self.nextValidOrderId += 1
            return oid
        
    # def place_order(self):
    #     self.simplePlaceOid = self.nextValidId()
    #     self.placeOrder(self.simplePlaceOid, ContractSamples.USStock(),
    #                     OrderSamples.LimitOrder("SELL", 1, 50))

def main():
    app = IBApp()
    app.connect("127.0.0.1", 7497, 0) #4002 for gateway, 7497 for TWS
    
    # contract.symbol = "AAPL"
    #     contract.secType = "STK"
    #     contract.exchange = "SMART"
    #     contract.currency = "USD"
    #     contract.primaryExchange = "NASDAQ"
    # app.reqContractDetails(10, contract)
    # app.queryDisplayGroups(9006)
    #app.reqAccountSummary(9006, "All", "AccountType")
    #app.reqPositions()
    app.start()
    # app.run()
    # # app.place_order()
    # app.reqAccountSummary(tags=["AccountType", "TotalCashValue"])
    

if __name__ == "__main__":
    main()