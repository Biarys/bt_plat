import Core.platform_core as bt
import Core.Settings as Settings
from Core.indicators import SMA

if __name__ == "__main__":
    Settings.read_from_csv_path = r"E:\Windows\Documents\bt_plat\stock_data\AA.csv"
    Settings.read_from = "csvFile"
    
    Settings.buy_delay = 1
    Settings.sell_delay = 1

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
    s.run()
    s.trade_list.to_csv("test.csv")