import Backtest.platform_core as bt
import Backtest.Settings as Settings
from Backtest.indicators import SMA
from auto_trading import automated_trading as at
from Backtest.data_reader import DataReader

from ibapi.contract import Contract
from ibapi.order import Order

import threading
import json
from datetime import datetime as dt


def run_every_min(strat, data):
    prev_min = None
    while True:
        now = dt.now()
        recent_min = now.minute
        if now.second == 1 and recent_min != prev_min:
            prev_min = recent_min
            strat.run(data)
            print("Running strategy")

if __name__ == "__main__":
    # Settings.read_from_csv_path = r"E:\Windows\Documents\bt_plat\stock_data\XOM.csv"
    # Settings.read_from = "csvFile"
    
    # Settings.buy_delay = 0
    # Settings.sell_delay = 0

    class Strategy(bt.Backtest):
        def logic(self, current_asset):
            
            sma5 = SMA(current_asset, ["Close"], 5)
            sma25 = SMA(current_asset, ["Close"], 25)

            buyCond = sma5() > sma25()
            sellCond = sma5() < sma25()
            
            shortCond = None
            coverCond = None

            return buyCond, sellCond, shortCond, coverCond
    
    s = Strategy("Test_SMA")
    # s.run()
    # s.trade_list = s.trade_list.round(2)
    # s.trade_list.to_csv("trades.csv")
    # s.port.value.round(2).to_csv("avail_amount.csv")

    # data = DataReader()

    with open(Settings.path_to_mapping, "r") as f:
        file = json.loads(f.read())

    app = at.IBApp()
    app.connect("127.0.0.1", 7497, 0) #4002 for gateway, 7497 for TWS
    app.start()
    #app.reqAccountSummary(9006, "All", "AccountType")

    # app.reqIds(-1)
    #app.placeOrder(app.nextOrderId(), at.IBContract.EurGbpFx(), at.IBOrder.MarketOrder("BUY", 500))
    # app.nextOrderId()
    # app.placeOrder(app.nextOrderId(), at.IBContract.EurGbpFx(), at.IBOrder.LimitOrder("BUY", 500, 0.85))
    # app.placeOrder(app.nextOrderId(), at.IBContract.EurGbpFx(), at.IBOrder.Stop("BUY", 500, 0.87))
    # print(app.nextValidOrderId)
    # app.reqOpenOrders()
    # app.cancelOrder(10)
    # app.reqPositions()
    app.reqHistoricalData(reqId=app.nextOrderId(), 
                        contract=at.IBContract.forex(file["forex"]["EURGBP"]),
                        #endDateTime="20191125 00:00:00",
                        endDateTime="",
                        durationStr="1800 S", 
                        barSizeSetting="1 min",
                        whatToShow="MIDPOINT",
                        useRTH=1,
                        formatDate=1,
                        keepUpToDate=True,
                        chartOptions=[]
                        )
    app.reqHistoricalData(reqId=app.nextOrderId(), 
                        contract=at.IBContract.forex(file["forex"]["EURUSD"]),
                        #endDateTime="20191125 00:00:00",
                        endDateTime="",
                        durationStr="1800 S", 
                        barSizeSetting="1 min",
                        whatToShow="MIDPOINT",
                        useRTH=1,
                        formatDate=1,
                        keepUpToDate=False,
                        chartOptions=[]
                        )
    # print(app.data_tracker)
    # print(app.data)
    run_every_min(s, app.data)
