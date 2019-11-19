from datetime import datetime as dt
import logging
import os
import Settings as settings

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract


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
# ? reqOpenOrders()                                         position
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

class Contract:

    @staticmethod
    def stock(symbol, secType, exchange, currency, primaryExchange):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        contract.primaryExchange = primaryExchange
        return contract

class Order:

    @staticmethod
    def MarketOrder(action, quantity):
        order = Order()
        order.action = action
        order.orderType = "MKT"
        order.totalQuantity = quantity
        return order

class _IBWrapper(EWrapper):
    def __init__(self):
        EWrapper.__init__(self)

class _IBClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)

class IBApp(_IBWrapper, _IBClient):
    def __init__(self):
        _IBWrapper.__init__(self)
        _IBClient.__init__(self, self)

        self.started = False

    @staticmethod
    def setup_log():
        if not os.path.exists(settings.log_path):
            try:
                print(f"Creating log folder in {settings.log_path}")
                os.mkdir(settings.log_path)
            except Exception as e:
                print(f"Failed to create log folder in {settings.log_path}")
                print(f"An error occured {e}")

        
        logging.basicConfig(filename=(settings.log_path+r"\test1.log"), format='%(asctime)s %(message)s',
                            level=logging.INFO)
        logging.info("Started")

    def start(self):
        if self.started:
            return

        print(logging.getLogger(__name__))
        self.started = True
        self.setup_log()
        self.run()
        

    def error(self, reqId, errorCode, errorString):
        print("Error: ", reqId, " ", errorCode, " ", errorString)

    def contractDetails(self, reqId, contractDetails):
        print("contractDetails: ", reqId, " ", contractDetails)

    def accountSummary(self, reqId, account, tag, value, currency):
        print(reqId, account, tag, value, currency)

    # def nextValidId(self, orderId):
    #     """
    #     The nextValidId event provides the next valid identifier needed to place an order. 
    #     This identifier is nothing more than the next number in the sequence. 
    #     This means that if there is a single client application submitting orders to an account, 
    #     it does not have to obtain a new valid identifier every time it needs to submit a new order. 
    #     It is enough to increase the last value received from the nextValidId method by one.

    #     However if there are multiple client applications connected to one account, 
    #     it is necessary to use an order ID with new orders which is greater than all previous 
    #     order IDs returned to the client application in openOrder or orderStatus callbacks.

    #     More info http://interactivebrokers.github.io/tws-api/order_submission.html
    #     """
    #     super().nextValidId(orderId)
    #     # logging.debug("setting nextValidOrderId: %d", orderId)
    #     self.nextValidOrderId = orderId
    #     print("NextValidId: ", orderId)
        
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
    app.queryDisplayGroups(9006)
    # app.reqAccountSummary(9006, "All", "AccountType")
    app.start()
    # app.run()
    # # app.place_order()
    # app.reqAccountSummary(tags=["AccountType", "TotalCashValue"])
    

if __name__ == "__main__":
    main()