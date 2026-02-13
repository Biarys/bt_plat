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

import warnings
# warnings.simplefilter("ignore", category=pd.errors.Pandas4Warning)
warnings.simplefilter("ignore")
warnings.filterwarnings("ignore")

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

class StrategySMALong(bt.Backtest):
        def __init__(self, name):
            super().__init__(name)
            self.stop_length = pd.DataFrame()

        def logic(self, current_asset, name):
            
            sma5 = SMA(current_asset, "Close", 5)
            sma25 = SMA(current_asset, "Close", 25)

            self.cond.buy = sma5() > sma25()
            self.cond.sell = sma5() < sma25()

class StrategySMAShort(bt.Backtest):
        def __init__(self, name):
            super().__init__(name)
            self.stop_length = pd.DataFrame()
        def logic(self, current_asset, name):
            
            sma5 = SMA(current_asset, "Close", 5)
            sma25 = SMA(current_asset, "Close", 25)

            self.cond.short = sma5() < sma25()
            self.cond.cover = sma5() > sma25()

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
    return trade_list

def run_strategy_test(strategy_class, baseline_path, data_path, is_portfolio):
    baseline = pd.read_excel(baseline_path, sheet_name="Tests")
    baseline["Ex. date"] = baseline["Ex. date"].astype(str)
    
    settings.read_from_csv_path = data_path
    if is_portfolio:
        settings.read_from = "csv_files"
    else:
        settings.read_from = "csv_file"

    data = ReaderFactory(settings.read_from, settings.read_from_csv_path)
    
    s = strategy_class("Test_SMA")
    s.run(data)
    
    s.trade_list = format_trade_list(s.trade_list)
    
    s.trade_list["Symbol"] = s.trade_list["Symbol"].str.replace(".csv", "")
    s.trade_list['Shares'] = s.trade_list['Shares'].astype(int)
    s.trade_list['Ex. date'] = pd.to_datetime(s.trade_list['Ex. date'], errors='coerce').dt.date.fillna("Open").values
    baseline['Ex. date'] = pd.to_datetime(baseline['Ex. date'], errors='coerce').dt.date.fillna("Open").values

    return baseline, s.trade_list

class TestRefactoredSMA(unittest.TestCase):
    maxDiff = None
    
    @classmethod
    def setUpClass(cls):
        cls.stock_list = ["AA", "AAPL", "DDD", "DY", "JPM", "T"]
        cls.path = os.getcwd()

    def compare_dfs(self, baseline, new):
        temp = compdf(baseline, new)
        if temp is not None: print(temp) 
        npt.assert_equal(baseline.values, new.values)

    def test_stock_strategies(self):
        test_cases = [
            {"strategy": StrategySMALong, "type": "Long", "folder": "Long"},
            {"strategy": StrategySMAShort, "type": "Short", "folder": "Short"}
        ]

        for test in test_cases:
            for name in self.stock_list:
                with self.subTest(f"{test['type']} strategy for {name}"):
                    baseline_path = os.path.join(self.path, f"Tests/{test['folder']}/baseline_sma_5_25_{name}.xlsx")
                    if test['type'] == 'Short':
                        baseline_path = os.path.join(self.path, f"Tests/{test['folder']}/baseline_short_sma_5_25_{name}.xlsx")

                    data_path = os.path.join(self.path, f"stock_data/{name}.csv")
                    
                    baseline, result = run_strategy_test(test["strategy"], baseline_path, data_path, is_portfolio=False)
                    self.compare_dfs(baseline, result)

    def test_portfolio_strategies(self):
        test_cases = [
            {"strategy": StrategySMALong, "type": "Long", "folder": "Long", "file": "baseline_sma_5_25_portfolio_excl_XOM.xlsx"},
            {"strategy": StrategySMAShort, "type": "Short", "folder": "Short", "file": "baseline_short_sma_5_25_portfolio_excl_XOM.xlsx"}
        ]

        for test in test_cases:
            with self.subTest(f"Portfolio {test['type']} strategy"):
                baseline_path = os.path.join(self.path, f"Tests/{test['folder']}/{test['file']}")
                data_path = os.path.join(self.path, "stock_data")
                
                baseline, result = run_strategy_test(test["strategy"], baseline_path, data_path, is_portfolio=True)
                self.compare_dfs(baseline, result)


if __name__=="__main__":
    settings.backtest_engine = "pandas"
    with warnings.catch_warnings(action='ignore'):
        # warnings.simplefilter("ignore")
        unittest.main()