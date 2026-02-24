import pandas as pd
import numpy as np
import logging
# for testing
from datetime import datetime as dt

# own files
from Backtest.indicators import SMA
from Backtest.data_reader import ReaderFactory, prepare_data
from Backtest.log import setup_log
from Backtest.settings import Settings
from Backtest.portfolio import Portfolio
from Backtest.engines import PandasEngine, SparkEngine
from Backtest import constants as C
from Backtest.processing import Agg_Trades, Agg_TransPrice
from Backtest.results import build_trade_list

logger = logging.getLogger(__name__)

setup_log()
#############################################
# Core starts
#############################################
class Backtest():
    def __init__(self, name="Test", real_time=False):
        self.name = name
        self.data = {}
        self.runs_at = dt.now() # for logging and data prep purposes. Gets updated when self.run() is called
        self.agg_trans_prices = Agg_TransPrice()
        self.agg_trades = Agg_Trades()
        self.agg_custom_stop = pd.DataFrame()
        self.agg_stop_length = pd.DataFrame()
        self.custom_stop_size = None
        self.trade_list = None
        self.universe_ranking = pd.DataFrame()
        self.real_time = real_time
        self.keys = None
        if Settings.backtest_engine.lower() == "pandas":
            self.engine = PandasEngine(self)
        elif Settings.backtest_engine.lower() == "spark":
            self.engine = SparkEngine(self)
        
    def preprocessing(self, data):
        """
        Called once before running through the data.
        Should be used to generate values that need to be known prior running logic such as ranking among asset classes.
        For example, if we want to know top 10 momentum stocks every month or highest stocks above 200 MA, this data can be generated at this stage.

        Inputs: self + all data that will be used for the backtest
        """
        logger.info("Preprocessing started.")
        return "break"

    def postprocessing(self, data):
        """
        Called right after logic.
        Should be used to generated values that depend on buy/sell/short/cover signals such as stops/take profits.

        Inputs: self + all data that will be used for the backtest
        """
        logger.info("Postprocessing started.")
        pass

    def run(self, data):
        logger.info(f"Running backtest '{self.name}'.")
        try:
            self.runs_at = dt.now()
            if self.real_time:
                for name in data.data:
                    data.data[name] = prepare_data(data.data, name, self.runs_at)

            self._run_portfolio(data)
            logger.info(f"Backtest '{self.name}' finished.")
        except Exception as e:
            logger.exception(e)
            
    def logic(self, data):
        pass

    def _run_portfolio(self, data):
        """
        Calculate profit and loss for the strategy
        """
        try:
            logger.info("Backtester started!")
            self.engine.run(data) # get data for single assets
            
            # prepare data for portfolio
            self.idx = self.agg_trades.priceFluctuation_dollar.index
            self.idx = pd.Index(self.idx, dtype=object)
            self.keys = [name.split(".csv")[0] for name in data.keys]
            # check to assure order of columns is the same among all dataframes. Otherwise results will be wrong
            # ! needs to be change from hardcoded buyPrice cuz can be empty
            # TODO: change this to be more dynamic and check all columns / something better
            assert all(self.keys == self.agg_trans_prices.buyPrice.columns), "self.keys are not identical among dataframes"
            
            # nan in the beg cuz of .shift while finding priceFluctuation
            # to avoid nan in the beg
            self.agg_trades.priceFluctuation_dollar.iloc[0] = 0

            self.portfolio = Portfolio(self.agg_trades.priceFluctuation_dollar, self.idx, self.keys)
            self.portfolio.run_portfolio(self.agg_trans_prices, self.agg_custom_stop)

            self.trade_list = build_trade_list(
                self.agg_trades.trades,
                self.idx,
                self.keys,
                self.portfolio.weights,
                Settings.start_amount,
            )
        except Exception as e:
            logger.exception(e)

    def apply_stop(self, buy_or_short, current_asset, stop_length, trail="false"):
        logger.info(f"Applying stop for {buy_or_short}.")
        if buy_or_short == "buy":
            temp_ind = current_asset[C.CLOSE] - stop_length
            temp = temp_ind[self.cond.buy==1]
        elif buy_or_short == C.SHORT:
            temp_ind = current_asset[C.CLOSE] + stop_length
            temp = temp_ind[self.cond.short==1]

        stops = pd.DataFrame(index=current_asset.index)
        # temp = temp_ind[cond==1]
        temp.name = "stop"
        stops = stops.join(temp)
        stops = stops.ffill()
        stops = stops["stop"]

        # update sell/cover cond
        if buy_or_short == "buy":
            temp_cond = current_asset[C.LOW] < stops
            self.cond.sell = temp_cond | self.cond.sell
        elif buy_or_short == C.SHORT:
            temp_cond = current_asset[C.HIGH] > stops
            self.cond.cover = temp_cond | self.cond.cover


if __name__ == "__main__":
    print("=======================")
    print("Run from main.py file!")
    print("=======================")
    Settings.read_from_csv_path = r"E:\Windows\Documents\bt_plat\stock_data\AA.csv"
    Settings.read_from = "csvFile"
    Settings.buy_delay = 0
    Settings.sell_delay = 0

    class Strategy(Backtest):
        def logic(self, data):
            
            sma5 = SMA(data, [C.CLOSE], 5)
            sma25 = SMA(data, [C.CLOSE], 25)

            buyCond = sma5() > sma25()
            sellCond = sma5() < sma25()
            
            shortCond = sma5() < sma25()
            coverCond = sma5() > sma25()

            return buyCond, sellCond, shortCond, coverCond
    
    s = Strategy("name")
    data = ReaderFactory(Settings.read_from, Settings.read_from_csv_path)
    s.run(data.data)
    # s.trade_list.to_csv("test.csv")
    # print(s.trade_list)
    # b = Backtest("Strategy 1")
    # # b.read_from_db
    # # strategy logic
    # b.run()
    # print(b.trade_list)
    # # b.show_results
