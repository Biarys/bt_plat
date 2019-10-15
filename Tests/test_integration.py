import sys
import pandas as pd
import pandas.testing as pdt
import numpy.testing as npt
import numpy as np
import os
import unittest

sys.path.append(sys.path[0] + "/..")
import Core.platform_core as bt
import Core.Settings as Settings
from Core.indicators import SMA

class TestStocks(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.stock_list = ["AA", "AAPL", "DDD", "DY", "JPM", "T", "XOM"]

    def compare_dfs(self, baseline, new):
        npt.assert_equal(baseline.values, new.values)

class TestSMA(TestStocks):
    def test_stock(self):
        path = os.getcwd()
        for name in self.stock_list:
            baseline = pd.read_excel(path + r'\Tests\baseline_sma_5_25_{name}.xlsx'.format(name=name), sheet_name="Tests")
            Settings.read_from_csv_path = path + r"\stock_data\{name}.csv".format(
                name=name)
            Settings.read_from = "csvFile"
            
            s = StrategySMA("Test_SMA")
            s.run()
            
            s.trade_list.rename(columns={
                "Date_entry":"Date",
                "Date_exit":"Ex. date",
                "Direction":"Trade",
                "Entry_price":"Price",
                "Exit_price":"Ex. Price",
                "Weight":"Shares",
                "Pct_change":"% chg",
                "Dollar_profit":"Profit",
                "Pct_profit":"% Profit",
                "Cum_profit":"Cum. Profit",
                "Position_value":"Position value"
            }, inplace=True)

            s.trade_list = s.trade_list[[
                "Symbol",
                "Trade",
                "Date",
                "Price",	
                "Ex. date",	
                "Ex. Price",	
                "% chg",
                "Profit",	
                "% Profit",	
                "Shares",
                "Position value",	
                "Cum. Profit"
            ]]
            s.trade_list[["Price", "Ex. Price", "Profit", "Position value", "Cum. Profit"]] = s.trade_list[
                ["Price", "Ex. Price", "Profit", "Position value", "Cum. Profit"]].round(2)
            s.trade_list[["% chg", "% Profit"]] = s.trade_list[["% chg", "% Profit"]].round(4)

            s.trade_list.to_csv("test.csv")

            self.compare_dfs(baseline, s.trade_list)

    def test_portfolio(self):
        path = os.getcwd()
        baseline = pd.read_excel(path + r'\Tests\baseline_sma_5_25_portfolio.xlsx')
        Settings.read_from_csv_path = path + r"\stock_data"
        Settings.read_from = "csvFiles"
        self.compare_dfs(baseline)

class StrategySMA(bt.Backtest):
        def logic(self, current_asset):
            
            sma5 = SMA(current_asset, ["Close"], 5)
            sma25 = SMA(current_asset, ["Close"], 25)

            buyCond = sma5() > sma25()
            sellCond = sma5() < sma25()
            
            shortCond = None
            coverCond = None

            return buyCond, sellCond, shortCond, coverCond

# might be useful for future testing
# df1 = pd.DataFrame([1,2,3], "a b c".split())
# df2 = pd.DataFrame([1,2,4], "a b c".split())
# rows, cols = np.intersect1d(df1.index, df2.index), np.intersect1d(df1.columns, df2.columns)
# same_df1 = df1.loc[rows, cols]
# same_df2 = df2.loc[rows, cols]
# sample output
#         0
# c  3 -> 4
# def compdf(x,y):
#     return (x.loc[~((x == y).all(axis=1)),
#                   ~((x == y).all(axis=0))][~(x==y)].applymap(str) +
#             ' -> ' +
#             y.loc[~((x == y).all(axis=1)),
#                   ~((x == y).all(axis=0))][~(x==y)].applymap(str)
#            ).replace('nan -> nan', ' ', regex=True)

if __name__=="__main__":
    # t = TestSMA()
    # t.test_stock()
    unittest.main()