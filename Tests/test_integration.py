import sys
import pandas as pd
import pandas.testing as pdt
import numpy.testing as npt
import numpy as np
import os

sys.path.append(sys.path[0] + "/..")
import Core.platform_core as bt
from  Core.indicators import SMA
import Core.Settings as Settings

class TestStocks():
    def __init__(self):
        self.stock_list = ["AA", "AAPL", "DDD", "DY", "JPM", "T", "XOM"]

    def compare_dfs(self, baseline, new):
        npt.assert_equal(baseline.values, new.values)

class TestSMA(TestStocks):
    def test_stock(self):
        path = os.getcwd()
        for name in self.stock_list:
            baseline = pd.read_excel(path + r'\Tests\baseline_sma_5_25_{name}.xlsx'.format(name=name))
            Settings.read_from_csv_path = path + r"\stock_data\{name}.csv".format(
                name=name)
            Settings.read_from = "csvFile"
            
            s = StrategySMA("name")
            s.run()

            self.compare_dfs(baseline, s.trade_list)

    # def test_AA(self):
    #     baseline = pd.read_excel('baseline_sma_5_25_AA.xlsx')
    #     self.compare_dfs(baseline)

    # def test_AAPL(self):
    #     baseline = pd.read_excel('baseline_sma_5_25_AAPL.xlsx')
    #     self.compare_dfs(baseline)

    # def test_DDD(self):
    #     baseline = pd.read_excel('baseline_sma_5_25_DDD.xlsx')
    #     self.compare_dfs(baseline)

    # def test_DY(self):
    #     baseline = pd.read_excel('baseline_sma_5_25_DY.xlsx')
    #     self.compare_dfs(baseline)

    # def test_JPM(self):
    #     baseline = pd.read_excel('baseline_sma_5_25_JPM.xlsx')
    #     self.compare_dfs(baseline)

    # def test_T(self):
    #     baseline = pd.read_excel('baseline_sma_5_25_T.xlsx')
    #     self.compare_dfs(baseline)

    # def test_XOM(self):
    #     baseline = pd.read_excel('baseline_sma_5_25_XOM.xlsx')
    #     self.compare_dfs(baseline)

    def test_portfolio(self):
        baseline = pd.read_excel('baseline_sma_5_25_XOM.xlsx')
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
    t = TestSMA()
    t.test_stock()
