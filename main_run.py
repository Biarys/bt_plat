import Backtest.platform_core as bt
import Backtest.Settings as Settings
from Backtest.indicators import SMA
from auto_trading import automated_trading as at
from Backtest.data_reader import DataReader
import Backtest.config as config

import json
from datetime import datetime as dt
import numpy as np



if __name__ == "__main__":
    Settings.read_from_csv_path = r"D:\HDF5\stocks_test.h5"
    # Settings.read_from = "hdf"
    
    # Settings.buy_delay = 0
    # Settings.sell_delay = 0

    class Strategy(bt.Backtest):
        def logic(self, current_asset):
            
            sma5 = SMA(current_asset, ["Close"], 2)
            sma25 = SMA(current_asset, ["Close"], 3)

            self.cond.buy = sma5() > sma25()
            self.cond.sell = sma5() < sma25()
            
            # self.cond.shortCond = None
            # self.cond.coverCond = None

            # return buyCond, sellCond, shortCond, coverCond
    

    # data = DataReader()
    # data.read_hdf_pd(Settings.read_from_csv_path)
    
    # s = Strategy("Test_SMA")
    # s.run(data.data)
    # s.trade_list = s.trade_list.round(2)
    # s.trade_list.to_csv("trades.csv")
    # s.port.value.round(2).to_csv("avail_amount.csv")
    # np.savetxt("avail_amount.csv", s.port.value, delimiter=",")


    

    app = at.IBApp()
    app.connect("127.0.0.1", 7497, 0) #4002 for gateway, 7497 for TWS
    app.start()
    #app.reqAccountSummary(9006, "All", "AccountType")


    app.reqHistoricalData(reqId=app.nextOrderId(), 
                        contract=at.IBContract.forex(app.asset_map["forex"]["EUR.GBP"]),
                        #endDateTime="20191125 00:00:00",
                        endDateTime="",
                        durationStr="1 D", 
                        barSizeSetting="1 min",
                        whatToShow="MIDPOINT",
                        useRTH=1,
                        formatDate=1,
                        keepUpToDate=True,
                        chartOptions=[]
                        )
    app.reqHistoricalData(reqId=app.nextOrderId(), 
                        contract=at.IBContract.forex(app.asset_map["forex"]["EUR.USD"]),
                        #endDateTime="20191125 00:00:00",
                        endDateTime="",
                        durationStr="1 D", 
                        barSizeSetting="1 min",
                        whatToShow="MIDPOINT",
                        useRTH=1,
                        formatDate=1,
                        keepUpToDate=True,
                        chartOptions=[]
                        )

    app.run_every_min(app.data, Strategy)
