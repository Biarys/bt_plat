import sys
import pandas as pd
import numpy.testing as npt
import os
import unittest


sys.path.append(sys.path[0] + "/..")
import Backtest.platform_core as bt
import Backtest.settings as settings
from Backtest.indicators import SMA
from Backtest.data_reader import ReaderFactory


def format_trade_list(trade_list):
    trade_list = trade_list.rename(columns={
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
    })

    trade_list = trade_list[[
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
    trade_list[["Price", "Ex. Price", "Profit", "Position value", "Cum. Profit"]] = trade_list[
        ["Price", "Ex. Price", "Profit", "Position value", "Cum. Profit"]].round(2)
    trade_list[["% chg", "% Profit"]] = trade_list[["% chg", "% Profit"]].round(4)
    trade_list["Ex. date"] = trade_list["Ex. date"].astype(str)
    trade_list["Symbol"] = trade_list["Symbol"].str.replace(".csv", "")
    trade_list['Shares'] = trade_list['Shares'].astype(int)
    # trade_list.sort_values(by="Ex. date", inplace=True)
    trade_list['Ex. date'] = pd.to_datetime(trade_list['Ex. date'], errors='coerce').dt.date.fillna("Open").values
    
    return trade_list
    
class TestStocks(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.stock_list = ["AA", "AAPL", "DDD", "DY", "JPM", "T"]

    def compare_dfs(self, baseline, new):
        # print(baseline.columns == new.columns)
        temp = compdf(baseline, new)
        if temp is not None: print(temp) 
        npt.assert_equal(baseline.values, new.values)

class TestSMA(TestStocks):

    maxDiff = None
    
    def test_stock_long(self):
        path = os.getcwd()
        for name in self.stock_list:
            print(f"RUNNING TEST FOR {name}")
            file_path = os.path.join(path, r'Tests/Long/baseline_sma_5_25_{name}.xlsx'.format(name=name))
            baseline = pd.read_excel(file_path, sheet_name="Tests")
            settings.read_from_csv_path = os.path.join(path, "stock_data")
        
            data = ReaderFactory("csv", settings.read_from_csv_path)

            s = StrategySMALong("Test_SMA")
            s.run(data)
            
            s.trade_list = format_trade_list(s.trade_list)
            baseline['Ex. date'] = pd.to_datetime(baseline['Ex. date'], errors='coerce').dt.date.fillna("Open").values
        
            self.compare_dfs(baseline, s.trade_list)

    def test_stock_short(self):
        path = os.getcwd()
        for name in self.stock_list:
            print(f"RUNNING TEST FOR {name}")
            baseline = pd.read_excel(path + r'\Tests\Short\baseline_short_sma_5_25_{name}.xlsx'.format(name=name), sheet_name="Tests")
            settings.read_from_csv_path = os.path.join(path, "stock_data")
        
            
            data = ReaderFactory("csv_file", settings.read_from_csv_path)

            s = StrategySMAShort("Test_SMA")
            s.run(data)
            
            s.trade_list = format_trade_list(s.trade_list)
            baseline['Ex. date'] = pd.to_datetime(baseline['Ex. date'], errors='coerce').dt.date.fillna("Open").values
        
            self.compare_dfs(baseline, s.trade_list)

    def test_portfolio_long(self):
        print("RUNNING PORTFOLIO TEST - LONG")
        path = os.getcwd()
        file_path = os.path.join(path, 'Tests/Long/baseline_sma_5_25_portfolio_excl_XOM.xlsx') 
        baseline = pd.read_excel(file_path, sheet_name="Tests")
        baseline["Ex. date"] = baseline["Ex. date"].astype(str)
        settings.read_from_csv_path = os.path.join(path, "stock_data")
        
        data = ReaderFactory("csv_files", settings.read_from_csv_path)

        s = StrategySMALong("Test_SMA")
        s.run(data)
        
        s.trade_list = format_trade_list(s.trade_list)
        # need these 2 lines to compare the results, else fails
        baseline['Ex. date'] = pd.to_datetime(baseline['Ex. date'], errors='coerce').dt.date.fillna("Open").values

        self.compare_dfs(baseline, s.trade_list)

    def test_portfolio_short(self):
        print("RUNNING PORTFOLIO TEST - SHORT")
        path = os.getcwd()
        file_path = os.path.join(path, 'Tests/Short/baseline_short_sma_5_25_portfolio_excl_XOM.xlsx')
        baseline = pd.read_excel(file_path, sheet_name="Tests")
        settings.read_from_csv_path = os.path.join(path, "stock_data")
        

        data = ReaderFactory("csv_files", settings.read_from_csv_path)
        # data.readCSVFiles(Settings.read_from_csv_path)
 
        s = StrategySMAShort("Test_SMA")
        s.run(data)
        
        s.trade_list = format_trade_list(s.trade_list)
        baseline['Ex. date'] = pd.to_datetime(baseline['Ex. date'], errors='coerce').dt.date.fillna("Open").values
        
        self.compare_dfs(baseline, s.trade_list)
            

class StrategySMALong(bt.Backtest):
        def __init__(self, name):
            super().__init__(name)
            self.stop_length = pd.DataFrame()

        def logic(self, current_asset, name):
            
            sma5 = SMA(current_asset, "Close", 5)
            sma25 = SMA(current_asset, "Close", 25)

            self.cond.buy = sma5() > sma25()
            self.cond.sell = sma5() < sma25()
            
            # shortCond = sma5() < sma25()
            # coverCond = sma5() > sma25()

            # return buyCond, sellCond, shortCond, coverCond

class StrategySMAShort(bt.Backtest):
        def logic(self, current_asset, name):
            

            sma5 = SMA(current_asset, "Close", 5)
            sma25 = SMA(current_asset, "Close", 25)

            self.cond.short = sma5() < sma25()
            self.cond.cover = sma5() > sma25()
            
            # shortCond = sma5() < sma25()
            # coverCond = sma5() > sma25()

            # return buyCond, sellCond, shortCond, coverCond

# might be useful for future testing
# df1 = pd.DataFrame([1,2,3], "a b c".split())
# df2 = pd.DataFrame([1,2,4], "a b c".split())
# rows, cols = np.intersect1d(df1.index, df2.index), np.intersect1d(df1.columns, df2.columns)
# same_df1 = df1.loc[rows, cols]
# same_df2 = df2.loc[rows, cols]
# sample output
#         0
# c  3 -> 4
def compdf(x,y):
    if not x.eq(y).all().all(): # compares each element, column, df
        try:
            return ((x.loc[~((x == y).all(axis=1)),
                        ~((x == y).all(axis=0))][~(x==y)].applymap(str) +
                    ' -> ' +
                    y.loc[~((x == y).all(axis=1)),
                        ~((x == y).all(axis=0))][~(x==y)].applymap(str)
                ).replace('nan -> nan', ' ', regex=True))
        except:
            return pd.concat([x, y]).loc[x.index.symmetric_difference(y.index)]

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestSMA('test_stock_long'))
    suite.addTest(TestSMA('test_portfolio_long'))
    # suite.addTest(TestSMA('test_stock_short'))
    # suite.addTest(TestSMA('test_portfolio_short'))
    return suite

if __name__=="__main__":
    settings.backtest_engine = "pandas"
    # t = TestSMA()
    # t.test_stock_long()
    # unittest.main()
    runner = unittest.TextTestRunner()
    runner.run(suite())