from datetime import datetime as dt
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import *


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


class IBApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

    def error(self, reqId, errorCode, errorString):
        print("Error: ", reqId, " ", errorCode, " ", errorString)

    def contractDetails(self, reqId, contractDetails):
        print("contractDetails: ", reqId, " ", contractDetails)

def main():
    app = IBApp()
    app.connect("127.0.0.1", 4002, 0)
    
    # contract.symbol = "AAPL"
    #     contract.secType = "STK"
    #     contract.exchange = "SMART"
    #     contract.currency = "USD"
    #     contract.primaryExchange = "NASDAQ"
    # app.reqContractDetails(10, contract)

    app.run()



if __name__ == "__main__":
    main()