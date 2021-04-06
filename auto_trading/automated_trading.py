from datetime import datetime as dt
import pandas as pd
import logging
import os
import threading
import time
from queue import Queue
import json
import smtplib, ssl 
from collections import defaultdict

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.scanner import ScannerSubscription

from auto_trading.other import send_email
from auto_trading import log
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

# def logall(logger):
#     def outer(func):
#         def inner(*args, **kwargs):
#             logger.info(*args, **kwargs)
#             return func(*args, **kwargs)
#         return inner
#     return outer

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

    @staticmethod
    def BracketOrder(parentOrderId:int, action:str, quantity:float, limitPrice:float,
                    takeProfitLimitPrice:float, stopLossPrice:float):
    
        #This will be our main or "parent" order
        parent = Order()
        parent.orderId = parentOrderId
        parent.action = action
        parent.orderType = "LMT"
        parent.totalQuantity = quantity
        parent.lmtPrice = round(limitPrice, 2)
        #The parent and children orders will need this attribute set to False to prevent accidental executions.
        #The LAST CHILD will have it set to True, 
        parent.transmit = False

        takeProfit = Order()
        takeProfit.orderId = parent.orderId + 1
        takeProfit.action = "SELL" if action == "BUY" else "BUY"
        takeProfit.orderType = "LMT"
        takeProfit.totalQuantity = quantity
        takeProfit.lmtPrice = round(takeProfitLimitPrice, 2)
        takeProfit.parentId = parentOrderId
        takeProfit.transmit = False

        stopLoss = Order()
        stopLoss.orderId = parent.orderId + 2
        stopLoss.action = "SELL" if action == "BUY" else "BUY"
        stopLoss.orderType = "STP"
        stopLoss.auxPrice = round(limitPrice + stopLossPrice, 2)
        stopLoss.triggerPrice = round(limitPrice - stopLossPrice, 2)
        stopLoss.adjustedOrderType = "STP"
        stopLoss.adjustedStopPrice = round(limitPrice, 2)
        # stopLoss.orderType = "TRAIL"
        # #Stop trigger price
        # stopLoss.auxPrice = round(stopLossPrice,2)
        stopLoss.totalQuantity = quantity
        stopLoss.parentId = parentOrderId
        #In this case, the low side order will be the last child being sent. Therefore, it needs to set this attribute to True 
        #to activate all its predecessors
        stopLoss.transmit = True

        bracketOrder = [parent, takeProfit, stopLoss]
        return bracketOrder

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
        scanSub.abovePrice = 2
        scanSub.belowPrice = 50
        scanSub.aboveVolume = 1000000

        return scanSub    
    
class _IBWrapper(EWrapper):
    def __init__(self):
        EWrapper.__init__(self)
        self.data = {}
        self.avail_funds = None
        self.scanner_instr = {}
        self.scanner_instr_all = {}
        self.daily_pnl = 0

    def error(self, reqId, errorCode, errorString):
        self.logger.error(f"ReqID: {reqId}, Code: {errorCode}, Error: {errorString}")
        if (reqId != -1) and (errorCode != 162):
            self.send_email(f"Subject: An error has occurred! \n\n ReqID: {reqId}, Code: {errorCode}, Error: {errorString}")

    def connectAck(self):
        if self.asynchronous:
            self.startApi()

    # commented out cuz not used
    # def contractDetails(self, reqId, contractDetails):
    #     # self.logger.info(f"ReqID: {reqId}, Contract Details: {contractDetails}")
    #     pass

    def accountSummary(self, reqId, account, tag, value, currency):
        if tag=="NetLiquidation":
            self.avail_funds = float(value)
        self.logger.info(f"ReqID: {reqId}, Account: {account}, Tag: {tag}, Value: {value}, Currency: {currency}")

    def accountSummaryEnd(self, reqId: int):
        self.logger.info(f"AccountSummaryEnd. ReqId: {reqId}")

    def pnl(self, reqId:int, dailyPnL:float, unrealizedPnL:float, realizedPnL:float):
        self.logger.info(f"Current daily P&L {dailyPnL}. ReqId: {reqId}")
        self.daily_pnl = dailyPnL
        self.daily_pnl_received = True

    def position(self, account, contract, pos, avg_cost):
        self.logger.info(f"Account: {account}, Contract: {contract.symbol}, Position: {pos}, Average cost: {avg_cost}")
        name = contract.symbol+"."+contract.currency
        # _row = pd.DataFrame(data=[[account, name, pos, avg_cost]], 
        #                     columns=["account", "symbol_currency", "quantity", "avg_cost"])
        # self.open_positions = self.open_positions.append(_row)
        self.open_positions[name] = {"symbol_currency":name, "quantity":pos, "avg_cost": avg_cost}

    def positionEnd(self):
        self.logger.info("Finished executing reqPositions")
        self.open_positions_received = True

    def openOrder(self, orderId, contract, order, orderState):
        # self.logger.info(f"Order Id: {orderId}, Contract: {contract.symbol}, Order: {order.action}, Commission paid: {orderState.commission}")
        name = contract.symbol+"."+contract.currency
        _row = pd.DataFrame(data=[[orderId, name, order.action, order.totalQuantity, order.orderType]], 
                            columns=["orderId", "symbol_currency", "buy_or_sell", "quantity", "order_type"])
        self.open_orders = self.open_orders.append(_row)
        # self.open_orders[orderId] = {"orderId":orderId, "symbol_currency":name, "order_action": order.action, "quantity":order.totalQuantity, "order_type": order.orderType}

    def openOrderEnd(self):
        self.logger.info("Finished executing reqOpenOrders")
        self.open_orders_received = True

    def completedOrder(self, contract, order, orderState):
        self.logger.info(f"Contract: {contract}. Order: {order}. OrderState: {orderState}")

    def completedOrderEnd(self):
        self.logger.info("Finished executing completedOrderEnd")

    def historicalData(self, reqId, bar):
        # delete old asset's data        
        if self._last_reqId != reqId:
            self._data_all = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
            self._last_reqId = reqId

        if reqId in self.data_tracker.keys():
            _date = pd.to_datetime(bar.date, format="%Y%m%d  %H:%M:%S") # note 2 spaces
            _row = pd.DataFrame(data=[[bar.open, bar.high, bar.low, bar.close, bar.volume]], 
                                columns=["Open", "High", "Low", "Close", "Volume"], index=[_date])
            self._data_all = self._data_all.append(_row)
            self._data_all.replace(to_replace=0, method='ffill', inplace=True) # add backward fill too?
            self._data_all.index.name = "Date"
            self.data[self.data_tracker[reqId]] = self._data_all

    def historicalDataUpdate(self, reqId, bar):
        self._data_all = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
        if self._last_reqId != reqId:
            self._data_all = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
            self._last_reqId = reqId
        if reqId in self.data_tracker.keys():
            _date = pd.to_datetime(bar.date, format="%Y%m%d  %H:%M:%S") # note 2 spaces
            _row = pd.DataFrame(data=[[bar.open, bar.high, bar.low, bar.close, bar.volume]], 
                                columns=["Open", "High", "Low", "Close", "Volume"], index=[_date])

            self._data_all = self._data_all.append(_row)
            self._data_all.index.name = "Date"
            self.data[self.data_tracker[reqId]] = self.data[self.data_tracker[reqId]].append(self._data_all)
            self.data[self.data_tracker[reqId]] = self.data[self.data_tracker[reqId]].replace(to_replace=0, method='ffill') # add backward fill too?
            if self.data[self.data_tracker[reqId]].eq(0).any(1).any():
                self.logger.warning(f"0s HAS BEEN FOUND IN THE DATA {reqId}: {_row}")

    def historicalDataEnd(self, reqId, start, end):
        self.logger.info(f"Historical Data End. ReqID: {reqId}, start: {start}, end: {end}")
        self.logger.info(f"Stocks that are being tracked: {self.data_tracker}")

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
        # if contractDetails.contract.symbol not in self.scanner_instr.keys():
        symbol = contractDetails.contract.symbol
        secType = contractDetails.contract.secType
        currency = contractDetails.contract.currency
        exchange = contractDetails.contract.exchange
        primaryExchange = contractDetails.contract.primaryExchange
        self.scanner_instr[symbol + "." + currency] = {"symbol": symbol,
                                                                "secType": secType,
                                                                "currency": currency,
                                                                "exchange": exchange,
                                                                "primaryExchange": primaryExchange
                                                                }
        self.scanner_instr_all[symbol + "." + currency] = {"symbol": symbol,
                                                                "secType": secType,
                                                                "currency": currency,
                                                                "exchange": exchange,
                                                                "primaryExchange": primaryExchange
                                                                }

    def scannerParameters(self, xml:str):
        print(xml)

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
        self.nextValidOrderId = orderId
        self.logger.info(f"NextValidId: {orderId}")


class _IBClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        self.data_tracker = {}

    def reqHistoricalData(self, reqId, contract, endDateTime="", durationStr="2 D", barSizeSetting="1 min", 
                        whatToShow="MIDPOINT", useRTH=1, formatDate=1, keepUpToDate=True, chartOptions=[]):
        super().reqHistoricalData(reqId, contract, endDateTime, durationStr, barSizeSetting, 
                                whatToShow, useRTH, formatDate, keepUpToDate, chartOptions)
        
        self.data_tracker[reqId] = contract.symbol + "." + contract.currency
        self.logger.info(self.data_tracker)

    def reqPositions(self):
        self.logger.info("Requesting open positions")
        super().reqPositions()
        self.open_positions_received = False

    def reqOpenOrders(self):        
        self.logger.info("Requesting open orders")
        super().reqOpenOrders()
        self.open_orders_received = False

    def reqAllOpenOrders(self):        
        self.logger.info("Requesting open orders")
        self.open_orders = pd.DataFrame(columns=["orderId", "symbol_currency", "buy_or_sell", "quantity", "order_type"])
        super().reqAllOpenOrders()
        self.open_orders_received = False

    def placeOrder(self, Id, contract, order):
        self.logger.info(f"Placing order for: id - {Id}, contract - {contract.symbol}, {contract.secType}, order - {order.action}, {order.orderType}")
        super().placeOrder(Id, contract, order)

    def reqPnL(self, reqId:int, account:str, modelCode:str):
        self.logger.info(f"Requesting PnL. ReqID: {reqId}")
        super().reqPnL(reqId, account, modelCode)
        self.daily_pnl_received = False

class IBApp(_IBWrapper, _IBClient):
    def __init__(self):
        _IBWrapper.__init__(self)
        _IBClient.__init__(self, wrapper=self)
        
        self.started = False
        self.nextValidOrderId = None
        self.logger = None
        self._data_all = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
        self._last_reqId = None
        self.open_orders = pd.DataFrame(columns=["orderId", "symbol_currency", "buy_or_sell", "quantity", "order_type"])
        self.open_positions = {}
        self.open_orders_received = False
        self.open_positions_received = False
        self.daily_pnl_received = False  

    def start(self):
        if self.started:
            return

        self.started = True
        log.setup_log("IBApp")
        self.logger = logging.getLogger("IBApp")

        #self.reqIds(-1) # to make sure nextValidOrderId gets a value for sure

        # with open(settings.path_to_mapping, "r") as f:
        #     self.asset_map = json.loads(f.read())
        
        ib_thread = threading.Thread(target=self.run, name="Interactive Broker Client Thread")
        ib_thread.start()
        self.logger.info("Waiting 1 second for nextValidID response")
        time.sleep(1) 

        self.req_account_data()
           
    def nextOrderId(self):
        if self.nextValidOrderId == None:
            self.reqIds(-1)
            time.sleep(1)
            self.nextOrderId()
        else:
            oid = self.nextValidOrderId
            self.logger.info(f"Next valid order id is: {self.nextValidOrderId}")
            self.nextValidOrderId += 1
            return oid
    
    def req_account_data(self):
        self.reqPositions()
        self.reqOpenOrders()
        self.reqPnL(self.nextOrderId(), settings.account_number, "")

    # def cancel_open_positions(self):
    #     self.reqPositions()

    #     while not self.open_positions_received:
    #         time.sleep(0.5)

    #     for ix, pos in self.open_positions.iterrows():
    #         self.placeOrder(self.nextOrderId(), IBContract.stock(self.asset_map["stock"][pos["symbol_currency"]]), IBOrder.MarketOrder("SELL", pos["quantity"]))

    def run_strategy(self, strat):
        prev_min = None
        from Backtest.data_reader import DataReader
        _run = True
        while _run:
            try:
                # now = dt.now()
                # recent_min = now.minute
                # if now.second == 5 and recent_min != prev_min:
                #     prev_min = recent_min
                if bool(self.data): # empty dict == False. Not empty == True
                    if (settings.account_stop_use and self.daily_pnl > self.calc_account_max_loss()) or (not settings.account_stop_use):
                        self.logger.info("Running strategy")
                        s = strat(real_time=True) # gotta create new object, otherwise it duplicates previous results  
                        data_ = DataReader("at", self.data) 
                        settings.start_amount = self.avail_funds
                        s.run(data_)
                        self.submit_orders(s.trade_list)
                    elif (settings.account_stop_use and self.daily_pnl <= self.calc_account_max_loss()):
                        self.send_email(f"Subject: WARNING: DAILY MAX LOSS HAS BEEN TRIGGERED! \n\n Daily PnL of {self.daily_pnl} has exceeded the threshold of {self.calc_account_max_loss()} {settings.account_stop_type}s. Terminating trading for the day.")
                        self.reqGlobalCancel() #Cancels all active orders. This method will cancel ALL open orders including those placed directly from TWS. 
                        self.close_open_positions()
                        self.logger.warning(f"Daily PnL of {self.daily_pnl} has exceeded the threshold of {self.calc_account_max_loss()} {settings.account_stop_type}s. Terminating trading for the day.") 
                        _run = False
            except Exception as e:
                self.logger.error("An error occured during run_strategy")
                self.logger.error(e, stack_info=True)

    @staticmethod
    def send_email(message):
        if settings.send_email:
            port = 465 # for SSL
            password = config.password
            smtp_server = "smtp.gmail.com"
            sender_email = config.sender_email  # Enter your address
            receiver_email = config.receiver_email  # Enter receiver address

            context = ssl.create_default_context() # Create a secure SSL context
            with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
                server.login(sender_email, password)
                for email in receiver_email:
                    server.sendmail(sender_email, email, message)

    def calc_account_max_loss(self):
        if settings.account_stop_type == "pct":
            return settings.account_stop_value * self.avail_funds
        elif settings.account_stop_type == "dollar":
            return settings.account_stop_value
        elif settings.account_stop_type == None:
            self.logger.error("=====================================================================")
            self.logger.error("account_stop_type in settings has not been specified while account_stop_use was set to True")
            self.logger.error("Please specify account_stop_type in the settings")
            self.logger.error("=====================================================================")

    def close_open_positions(self):
        current_positions = self._active_open_positions()
        
        for ix, order in current_positions.iterrows():
            asset = order["symbol_currency"]
            try:                
                self.logger.info("TERMINATING ALL OPEN POSITIONS")
                if order["quantity"] > 0:
                    self.placeOrder(self.nextOrderId(), IBContract.stock(self.scanner_instr_all[asset]), IBOrder.MarketOrder("SELL", order["quantity"]))
                    self.send_email(f"Subject: TERMINATING OPEN POSITION FOR {asset} \n\n Maximum account loss has been triggered. Closing long position for {asset}. Position size: {order['quantity']}")
                elif order["quantity"] < 0:
                    self.placeOrder(self.nextOrderId(), IBContract.stock(self.scanner_instr_all[asset]), IBOrder.MarketOrder("BUY", abs(order["quantity"])))
                    self.send_email(f"Subject: TERMINATING OPEN POSITION FOR {asset} \n\n Maximum account loss has been triggered. Closing short position for {asset}. Position size: {order['quantity']}")
            except Exception as e:
                self.send_email(f"Subject: COULDNT TERMINATE POSITION FOR {asset} \n\n An error has occured during exit termination logic: {e}")
                self.logger.error(f"COULDNT TERMINATE POSITION FOR {asset}. An error has occured during exit termination logic: {e}")
                self.logger.error(e, stack_info=True)

    def submit_orders(self, trades):
        bt_orders = trades[trades["Date_exit"] == "Open"]
        
        # Requests all current open orders in associated accounts at the current moment. 
        # Open orders are returned once; this function does not initiate a subscription. 
        self.reqAllOpenOrders()

        while not self.open_orders_received:
            time.sleep(0.5)

        current_orders = self.open_orders
        current_positions = self._active_open_positions()
        
        print(f"Open positions: {current_positions}")
        print(f"Open orders: {current_orders}")
        # if len(bt_orders) == 0:
        #     # close all positions and orders
        #     self.reqGlobalCancel() #Cancels all active orders. This method will cancel ALL open orders including those placed directly from TWS. 
        #     self.cancelOpenPositions()
        #     # ! need to find a better way of notifying open pos cancelled. Right now send email unless we have open positions
        #     # self.send_email(f"Subject: Open Positions Cancelled. \n\n Orders has been cancelled: {self.open_orders}. Positions have been cancelled {self.open_positions}")
        # else:
        # entry logic
        for ix, order in bt_orders.iterrows():
            asset = order["Symbol"]
            try:
                # simple check to see if current order has already been submitted
                if (asset not in current_orders["symbol_currency"].values) and (asset not in current_positions["symbol_currency"].values):
                    self.logger.info("Calling entry logic")
                    if bt_orders[bt_orders["Symbol"]==asset]["Direction"].iloc[0] == "Long":
                        self.placeOrder(self.nextOrderId(), IBContract.stock(self.scanner_instr_all[asset]), IBOrder.MarketOrder("BUY", order["Weight"]))
                        self.send_email(f"Subject: Buy signal for {asset} \n\n Open long for {asset}. Position size: {order['Weight']}. Trigger price (theoretical price): {order['Entry_price']}")
                    
                    elif bt_orders[bt_orders["Symbol"]==asset]["Direction"].iloc[0] == "Short":
                        # self.placeOrder(self.nextOrderId(), IBContract.stock(self.scanner_instr_all[asset]), IBOrder.MarketOrder("SELL", abs(order["Weight"])))
                        oid = self.nextOrderId()
                        self.placeOrder(oid, IBContract.stock(self.scanner_instr_all[asset]), IBOrder.BracketOrder(oid, "SELL",  abs(order["Weight"]), )) 
                    #     (parentOrderId:int, action:str, quantity:float, takeProfitLimitPrice:float, stopLossPrice:float)
                        self.send_email(f"Subject: Short signal for {asset} \n\n Open short for {asset}. Position size: {order['Weight']}. Trigger price (theoretical price): {order['Entry_price']}")
            except Exception as e:
                self.send_email(f"Subject: Couldnt enter position for {asset} \n\n An error has occured during entry logic: {e}")
                self.logger.error(f"Couldnt enter position for {asset}. An error has occured during entry logic: {e}")
                self.logger.error(e, stack_info=True)
                continue
        # exit logic
        for ix, order in current_positions.iterrows():
            asset = order["symbol_currency"]
            try:                
                if (asset not in bt_orders["Symbol"].values) and (asset not in current_orders["symbol_currency"].values):
                    self.logger.info("Calling exit logic")
                    if order["quantity"] > 0:
                        self.placeOrder(self.nextOrderId(), IBContract.stock(self.scanner_instr_all[asset]), IBOrder.MarketOrder("SELL", order["quantity"]))
                        self.send_email(f"Subject: Sell signal for {asset} \n\n Close long for {asset}. Position size: {order['quantity']}")

                    elif order["quantity"] < 0:
                        self.placeOrder(self.nextOrderId(), IBContract.stock(self.scanner_instr_all[asset]), IBOrder.MarketOrder("BUY", abs(order["quantity"])))
                        self.send_email(f"Subject: Cover signal for {asset} \n\n Close short for {asset}. Position size: {order['quantity']}")
            except Exception as e:
                self.send_email(f"Subject: Couldnt exit position for {asset} \n\n An error has occured during exit logic: {e}")
                self.logger.error(f"Couldnt exit position for {asset}. An error has occured during exit logic: {e}")
                self.logger.error(e, stack_info=True)
                continue

    def read_data(self, stock):
        return (stock, self.data[stock])

    def scannerDataEnd(self, reqId:int):
        # super().scannerDataEnd(self, reqId:int)
        for symbol in self.scanner_instr.keys():
            # Check if already requested & tracking data for the symbol
            # Otherwise it will request multiples of the same symbol -> reach limit of 50 simultaneous API historical data requests
            if symbol not in self.data_tracker.values():
                self.logger.info(f"SYMBOL NOT TRACKED: {symbol}, Currently tracking: {self.data_tracker.values()}")
                self.logger.info(f"Requesting data for: {symbol}")
                self.reqHistoricalData(reqId=self.nextOrderId(), contract=IBContract.stock(self.scanner_instr[symbol]))

        keys_to_pop = []
        # if dict not empty -> create df from it. Otherwise create an empty df
        if self.open_positions:
            current_positions = pd.DataFrame.from_dict(self.open_positions, orient="index")
            current_positions = current_positions[current_positions["quantity"] != 0] # also shows closed positions with quantity 0 for some reason 
        else:
            current_positions = pd.DataFrame(columns=["account", "symbol_currency", "quantity", "avg_cost"])

        for _reqId in self.data_tracker:
            try:
                symbol = self.data_tracker[_reqId]
                # Check if symbol does not need to be tracked
                # Otherwise keep running strategy on extra data (slow) + reach limit of 50 simultaneous API historical data requests
                if (symbol not in self.scanner_instr.keys()) and (symbol not in current_positions["symbol_currency"].values): #add symbol not in current open positions?
                    self.logger.info(f"Unsubscribing data for: {symbol}")
                    self.cancelHistoricalData(reqId=_reqId)
                    keys_to_pop.append(_reqId)
                    self.data.pop(symbol)
                    self.logger.info(f"REMOVING SYMBOL: {symbol}, Currently tracking: {self.data_tracker.values()}")
            except Exception as e:
                self.logger.error("An error has occured while removing symbols")
                self.logger.error(e, stack_info=True)
        # removing stocks that dont need to be tracked from data tracker
        [self.data_tracker.pop(key) for key in keys_to_pop]
        
        self.logger.info(f"Finished executing scanner data reqID: {reqId}")
        self.logger.info(f"Stocks in self.scanner_instr.keys(): {self.scanner_instr.keys()}")
        self.logger.info(f"Currently tracking: {self.data_tracker.values()}")
        # refresh scanner to receive only recently tracked data
        self.scanner_instr = {}

    def _active_open_positions(self):
        # if dict not empty -> create df from it. Otherwise create an empty df
        if self.open_positions:
            current_positions = pd.DataFrame.from_dict(self.open_positions, orient="index")
            current_positions = current_positions[current_positions["quantity"] != 0] # also shows closed positions with quantity 0 for some reason 
        else:
            current_positions = pd.DataFrame(columns=["account", "symbol_currency", "quantity", "avg_cost"])
        return current_positions

if __name__ == "__main__":
    print("You are not running from the main script!")