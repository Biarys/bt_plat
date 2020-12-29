from datetime import datetime as dt
import pandas as pd
import logging
import os
import threading
import time
from queue import Queue
import json
import smtplib, ssl 

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.scanner import ScannerSubscription

from auto_trading.other import send_email
from Backtest import Settings as settings
import Backtest.config as config

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


def _setup_log(name, level=logging.INFO):
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

    handler_file = logging.FileHandler(settings.log_folder + r"/" + settings.log_name, mode="w")
    handler_file.setLevel(level)
    handler_file.setFormatter(formatter)
    
    logger.addHandler(handler_console)
    logger.addHandler(handler_file)

    logger.info("Started")

class IBContract:

    @staticmethod
    def stock(ticker):
        contract = Contract()
        contract.symbol = ticker["symbol"]
        contract.secType = ticker["secType"]        
        contract.currency = ticker["currency"]
        contract.exchange = ticker["exchange"]
        contract.primaryExchange = ticker["primaryExchange"]
        return contract

    @staticmethod
    def forex(ticker):
        contract = Contract()
        contract.symbol = ticker["symbol"]
        contract.secType = ticker["secType"]
        contract.currency = ticker["currency"]
        contract.exchange = ticker["exchange"]
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

class IBScanner:
    def __init__(self):
        pass

    @staticmethod
    def HottestPennyStocks():
        """
        Subscribe to US stocks 1 < price < 10 and vol > 1M.
        Scan code = TOP_PERC_GAIN
        """
        scanSub = ScannerSubscription()
        scanSub.instrument = "STK"
        scanSub.locationCode = "STK.US"
        scanSub.scanCode = "TOP_PERC_GAIN"
        scanSub.abovePrice = 1
        scanSub.belowPrice = 10
        scanSub.aboveVolume = 1000000

        return scanSub
    
    
class _IBWrapper(EWrapper):
    def __init__(self):
        EWrapper.__init__(self)
        self.data = {}
        self.avail_funds = None
        self.scanner_instr = {}

    def error(self, reqId, errorCode, errorString):
        print(f"ReqID: {reqId}, Code: {errorCode}, Error: {errorString}")
        self.logger.error(f"ReqID: {reqId}, Code: {errorCode}, Error: {errorString}")

    @printall
    def contractDetails(self, reqId, contractDetails):
        print(f"ReqID: {reqId}, Contract Details: {contractDetails}")
        self.logger.info(f"ReqID: {reqId}, Contract Details: {contractDetails}")

    @printall
    def accountSummary(self, reqId, account, tag, value, currency):
        if tag=="NetLiquidation":
            self.avail_funds = float(value)
        # print(f"ReqID: {reqId}, Account: {account}, Tag: {tag}, Value: {value}, Currency: {currency}")
        # self.logger.info(f"ReqID: {reqId}, Account: {account}, Tag: {tag}, Value: {value}, Currency: {currency}")

    @printall
    def accountSummaryEnd(self, reqId: int):
        print(f"AccountSummaryEnd. ReqId: {reqId}")
        self.logger.info(f"AccountSummaryEnd. ReqId: {reqId}")

    @printall
    def position(self, account, contract, pos, avg_cost):
        print(f"Account: {account}, Contract: {contract}, Position: {pos}, Average cost: {avg_cost}")
        self.logger.info(f"Account: {account}, Contract: {contract}, Position: {pos}, Average cost: {avg_cost}")
        _row = pd.DataFrame(data=[[account, contract.symbol+"."+contract.currency, pos, avg_cost]], 
                            columns=["account", "symbol_currency", "quantity", "avg_cost"])
        self.open_positions = self.open_positions.append(_row)

    def positionEnd(self):
        print("Finished executing reqPositions")
        self.logger.info("Finished executing reqPositions")
        self.open_positions_received = True


    @printall
    def connectAck(self):
        # print("connectAck CALLED")
        if self.asynchronous:
            self.startApi()

    def openOrder(self, orderId, contract, order, orderState):
        print(f"Order Id: {orderId}, Contract: {contract}, Order: {order}, Order state: {orderState}")
        self.logger.info(f"Order Id: {orderId}, Contract: {contract}, Order: {order}, Order state: {orderState}")
        _row = pd.DataFrame(data=[[orderId, contract.symbol+"."+contract.currency, order.action, order.totalQuantity, order.orderType]], 
                            columns=["orderId", "symbol_currency", "buy_or_sell", "quantity", "order_type"])
        self.open_orders = self.open_orders.append(_row)

    def openOrderEnd(self):
        print("Finished executing reqOpenOrders")
        self.logger.info("Finished executing reqOpenOrders")
        self.open_orders_received = True

    def historicalData(self, reqId, bar):
        #print(f"ReqID: {reqId}, Hist Data: {bar}")
        # delete old asset's data        
        if self._last_reqId != reqId:
            self._data_all = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
            self._last_reqId = reqId
            
        # self.logger.info(f"ReqID: {reqId}, Hist Data: {bar}")

        _date = pd.to_datetime(bar.date, format="%Y%m%d  %H:%M:%S") # note 2 spaces
        _row = pd.DataFrame(data=[[bar.open, bar.high, bar.low, bar.close, bar.volume]], 
                            columns=["Open", "High", "Low", "Close", "Volume"], index=[_date])

        self._data_all = self._data_all.append(_row)
        self._data_all.index.name = "Date"
        # self.data[self.data_tracker[reqId]] = self.data[self.data_tracker[reqId]].append(_row)
        self.data[self.data_tracker[reqId]] = self._data_all
        # self.q.put(self.data)

    def historicalDataUpdate(self, reqId, bar):
        #print(f"ReqID: {reqId}, Hist Data: {bar}")
        self._data_all = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
        if self._last_reqId != reqId:
            self._data_all = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
            self._last_reqId = reqId
            
        # self.logger.info(f"ReqID: {reqId}, Hist Data: {bar}")

        _date = pd.to_datetime(bar.date, format="%Y%m%d  %H:%M:%S") # note 2 spaces
        _row = pd.DataFrame(data=[[bar.open, bar.high, bar.low, bar.close, bar.volume]], 
                            columns=["Open", "High", "Low", "Close", "Volume"], index=[_date])

        self._data_all = self._data_all.append(_row)
        self._data_all.index.name = "Date"
        self.data[self.data_tracker[reqId]] = self.data[self.data_tracker[reqId]].append(self._data_all)
        
    def historicalDataEnd(self, reqId, start, end):
        print(f"ReqID: {reqId}, start: {start}, end: {end}")
        # self.data = self.q.get()

    def scannerData(self, reqId:int, rank:int, contractDetails, distance:str, benchmark:str, projection:str, legsStr:str):
        """
        Parameters
            reqid	        the request's identifier.
            rank	        the ranking within the response of this bar.
            contractDetails	the data's ContractDetails
            distance	    according to query.
            benchmark	    according to query.
            projection	    according to query.
            legStr	        describes the combo legs when the scanner is returning EFP 
        """
        if contractDetails.contract.symbol not in self.scanner_instr.keys():
            symbol = contractDetails.contract.symbol
            secType = contractDetails.contract.secType
            currency = contractDetails.contract.currency
            exchange = contractDetails.contract.exchange
            primaryExchange = contractDetails.contract.primaryExchange
            self.scanner_instr[contractDetails.contract.symbol] = {"symbol": symbol,
                                                                   "secType": secType,
                                                                   "currency": currency,
                                                                   "exchange": exchange,
                                                                   "primaryExchange": primaryExchange
                                                                  }
        print(f"ReqId: {reqId}, rank: {rank}, symbol: {symbol}, secType: {secType}, exchange: {exchange}, primaryExchange: {primaryExchange}, currency: {currency}")

    def scannerParameters(self, xml:str):
        print(xml)

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

    def reqHistoricalData(self, reqId, contract, endDateTime="", durationStr="3 D", barSizeSetting="1 min", 
                        whatToShow="MIDPOINT", useRTH=1, formatDate=1, keepUpToDate=True, chartOptions=[]):
        super().reqHistoricalData(reqId, contract, endDateTime, durationStr, barSizeSetting, 
                                whatToShow, useRTH, formatDate, keepUpToDate, chartOptions)
        
        # temp = _split_contract(contract)
        # print(temp)
        self.data_tracker[reqId] = contract.symbol + "." + contract.currency
        print(self.data_tracker)

    def reqPositions(self):        
        print("Requesting open positions")
        self.logger.info("Requesting open positions")
        super().reqPositions()
        self.open_positions_received = False
        self.open_positions = pd.DataFrame(columns=["account", "symbol_currency", "quantity", "avg_cost"])

    def reqOpenOrders(self):        
        print("Requesting open orders")
        self.logger.info("Requesting open orders")
        super().reqOpenOrders()
        self.open_orders_received = False
        self.open_orders = pd.DataFrame(columns=["orderId", "symbol_currency", "buy_or_sell", "quantity", "order_type"])

class IBApp(_IBWrapper, _IBClient):
    def __init__(self):
        _IBWrapper.__init__(self)
        _IBClient.__init__(self, self)
        
        self.started = False
        self.nextValidOrderId = None
        self.logger = None   
        self.data_tracker = {}  
        self.data = {}  
        self._data_all = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
        self._last_reqId = None
        self.open_orders = pd.DataFrame(columns=["orderId", "symbol_currency", "buy_or_sell", "quantity", "order_type"])
        self.open_positions = pd.DataFrame(columns=["account", "symbol_currency", "quantity", "avg_cost"])
        self.open_orders_received = False
        self.open_positions_received = False
        # self.q = Queue()
        

    def start(self):
        if self.started:
            return

        self.started = True
        _setup_log("IBApp")
        self.logger = logging.getLogger("IBApp")

        #self.reqIds(-1) # to make sure nextValidOrderId gets a value for sure
 
        # self.reqAllOpenOrders()
        # self.reqCurrentTime()


        with open(settings.path_to_mapping, "r") as f:
            self.asset_map = json.loads(f.read())
        
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
        
    def cancel_open_positions(self):
        self.reqPositions()

        while not self.open_positions_received:
            time.sleep(0.5)

        for ix, pos in self.open_positions.iterrows():
            self.placeOrder(self.nextOrderId(), IBContract.stock(self.asset_map["stock"][pos["symbol_currency"]]), IBOrder.MarketOrder("SELL", pos["quantity"]))

    def run_strategy(self, strat):
        prev_min = None
        from Backtest.data_reader import DataReader
        while True:
            try:
                now = dt.now()
                recent_min = now.minute
                if now.second == 5 and recent_min != prev_min:
                    prev_min = recent_min
                    print("Running strategy")
                    s = strat(real_time=True) # gotta create new object, otherwise it duplicates previous results  
                    data_ = DataReader("at", self.data) 
                    settings.start_amount = self.avail_funds
                    s.run(data_)
                    self.submit_orders(s.trade_list)
            except Exception as e:
                print("An error occured")
                print(e)

    @staticmethod
    def send_email(message):
        port = 465 # for SSL
        password = config.password
        smtp_server = "smtp.gmail.com"
        sender_email = config.sender_email  # Enter your address
        receiver_email = config.receiver_email  # Enter receiver address

        # msg = MIMEText("""body""")
        # msg['To'] = ", ".join(receiver_email)
        # msg['Subject'] = "subject line"
        # msg['From'] = sender_email

        context = ssl.create_default_context() # Create a secure SSL context
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            for email in receiver_email:
                server.sendmail(sender_email, email, message)

    def cancelOpenPositions(self):
        self.reqPositions()

        while not self.open_positions_received:
            time.sleep(0.5)

        current_positions = self.open_positions[self.open_positions["quantity"] != 0] # also shows closed positions with quantity 0 for some reason 

        for ix, order in current_positions.iterrows():
            asset = order["symbol_currency"]

            if order["quantity"] > 0:
                self.placeOrder(self.nextOrderId(), IBContract.forex(self.asset_map["forex"][asset]), IBOrder.MarketOrder("SELL", order["quantity"]))
                self.send_email(f"Subject: Sell signal {asset} \n\n SELL - {asset} - {order['quantity']}")

            elif order["quantity"] < 0:
                self.placeOrder(self.nextOrderId(), IBContract.forex(self.asset_map["forex"][asset]), IBOrder.MarketOrder("BUY", abs(order["quantity"])))
                self.send_email(f"Subject: Cover signal {asset} \n\n COVER - {asset} - {order['quantity']}")

           

    def submit_orders(self, trades):
        self.reqPositions()
        self.reqOpenOrders()

        bt_orders = trades[trades["Date_exit"] == "Open"]

        while (not self.open_orders_received) and (not self.open_positions_received):
            time.sleep(0.5)
            
        current_orders = self.open_orders
        current_positions = self.open_positions[self.open_positions["quantity"] != 0] # also shows closed positions with quantity 0 for some reason 
        print(self.open_positions)
        # if len(bt_orders) == 0:
        #     # close all positions and orders
        #     self.reqGlobalCancel() #Cancels all active orders. This method will cancel ALL open orders including those placed directly from TWS. 
        #     self.cancelOpenPositions()
        #     # ! need to find a better way of notifying open pos cancelled. Right now send email unless we have open positions
        #     # self.send_email(f"Subject: Open Positions Cancelled. \n\n Orders has been cancelled: {self.open_orders}. Positions have been cancelled {self.open_positions}")
        # else:
        # entry logic
        for ix, order in bt_orders.iterrows():
            # _buy_or_sell = "BUY" if order["Direction"] == "Long" else "SELL"
            asset = order["Symbol"]
            # _asset = order["Symbol"].split(".")
            # _asset = "".join(_asset)
            try:
                # simple check to see if current order has already been submitted
                if (asset not in current_orders["symbol_currency"].values) and (asset not in current_positions["symbol_currency"].values):
                    print("Calling entry logic")
                    if bt_orders[bt_orders["Symbol"]==asset]["Direction"].iloc[0] == "Long":
                        self.placeOrder(self.nextOrderId(), IBContract.forex(self.asset_map["stock"][asset]), IBOrder.MarketOrder("BUY", order["Weight"]))
                        self.send_email(f"Subject: Buy signal {asset} \n\n BUY - {asset} - {order['Weight']}")
                    
                    elif bt_orders[bt_orders["Symbol"]==asset]["Direction"].iloc[0] == "Short":
                        self.placeOrder(self.nextOrderId(), IBContract.forex(self.asset_map["stock"][asset]), IBOrder.MarketOrder("SELL", abs(order["Weight"])))
                        self.send_email(f"Subject: Short signal {asset} \n\n SHORT - {asset} - {order['Weight']}")
            except Exception as e:
                print(f"Couldnt enter position for {asset}")
                print(e)
        # exit logic
        for ix, order in current_positions.iterrows():
            try:
                asset = order["symbol_currency"]
                if (asset not in bt_orders["Symbol"].values) and (asset not in current_orders["symbol_currency"].values):
                    print("Calling exit logic")
                    if order["quantity"] > 0:
                        self.placeOrder(self.nextOrderId(), IBContract.forex(self.asset_map["stock"][asset]), IBOrder.MarketOrder("SELL", order["quantity"]))
                        self.send_email(f"Subject: Sell signal {asset} \n\n SELL - {asset} - {order['quantity']}")

                    elif order["quantity"] < 0:
                        self.placeOrder(self.nextOrderId(), IBContract.forex(self.asset_map["stock"][asset]), IBOrder.MarketOrder("BUY", abs(order["quantity"])))
                        self.send_email(f"Subject: Cover signal {asset} \n\n COVER - {asset} - {order['quantity']}")
            except Exception as e:
                print(f"Couldnt exit position for {asset}")
                print(e)
    def read_data(self, stock):
        return (stock, self.data[stock])

    def scannerDataEnd(self, reqId:int):
        # super().scannerDataEnd(self, reqId:int)
        for symbol in self.scanner_instr.keys():
            self.reqHistoricalData(reqId=self.nextOrderId(), contract=IBContract.stock(self.scanner_instr[symbol]))
        print(f"Finished executing scanner data reqID: {reqId}")

    # def place_order(self):
    #     self.simplePlaceOid = self.nextValidId()
    #     self.placeOrder(self.simplePlaceOid, ContractSamples.USStock(),
    #                     OrderSamples.LimitOrder("SELL", 1, 50))


if __name__ == "__main__":
    print("You are not running from the main script!")