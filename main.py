import pandas as pd
import Backtest.platform_core as bt
from Backtest.indicators import SMA

class StrategySMALong(bt.Backtest):
        def __init__(self, name):
            super().__init__(name)
            self.stop_length = pd.DataFrame()

        def logic(self, current_asset, cond):
            
            sma5 = SMA(current_asset, "Close", 5)
            sma25 = SMA(current_asset, "Close", 25)

            cond.buy = sma5() > sma25()
            cond.sell = sma5() < sma25()
            
            # shortCond = sma5() < sma25()
            # coverCond = sma5() > sma25()

            # return buyCond, sellCond, shortCond, coverCond
