import Backtest.platform_core as bt
import Backtest.Settings as Settings
from Backtest.indicators import SMA
from Backtest.data_reader import DataReader

if __name__ == "__main__":
    Settings.backtest_engine = "pandas"
    Settings.read_from_csv_path = r"E:\Windows\Documents\bt_plat\stock_data"
    
    class Strategy(bt.Backtest):
        """
        Sample strategy
        """
        def logic(self, current_asset, name=None):
            sma5 = SMA(current_asset, "Close", 5)
            sma25 = SMA(current_asset, "Close", 25)

            self.cond.buy = sma5() > sma25()
            self.cond.sell = sma5() < sma25()

    data = DataReader("csv_files", Settings.read_from_csv_path) 
    s = Strategy("Test_SMA")
    s.run(data)

    print(s.trade_list)
            
    