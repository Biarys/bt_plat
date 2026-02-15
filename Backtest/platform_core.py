import pandas as pd
import numpy as np
import os
import abc
import logging
# import traceback
# for testing
from datetime import datetime as dt

# own files
from Backtest.indicators import SMA
from Backtest.data_reader import ReaderFactory
from Backtest.log import setup_log
from Backtest.settings import Settings
from Backtest.utils import _aggregate, _prep_and_agg_custom_stops, _find_df, _find_signals, _remove_dups
from Backtest.portfolio import Portfolio
from Backtest.engines import PandasEngine, SparkEngine
from Backtest import constants as C
from Backtest.processing import Agg_Trades, Agg_TransPrice, Cond, Repeater, TradeSignal, TransPrice, Trades

# if Settings.backtest_engine.lower() == "spark":
#     import pyspark
#     import pyspark.sql.functions as pySqlFunc
#     from pyspark.sql.functions import col
#     from pyspark.sql.window import Window
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
                    data.data[name] = self._prepare_data(data.data, name)

            self._run_portfolio(data)
            logger.info(f"Backtest '{self.name}' finished.")
        except Exception as e:
            # print(e)
            # traceback.print_exc()
            logger.error(e, stack_info=True)
            
    def logic(self, current_asset, name=None):
        logger.debug(f"Running logic for {name}.")
        pass

    def _prepare_data(self, data, name):
        logger.info(f"Preparing data for {name} - {self.runs_at}")
        # for name in data:
        temp = pd.DataFrame(columns=data[name].columns)
        temp.index.name = "Date"
        temp[C.OPEN] = data[name][C.OPEN].groupby(C.DATE).nth(0)
        temp[C.HIGH] = data[name][C.HIGH].groupby(C.DATE).max()
        temp[C.LOW] = data[name][C.LOW].groupby(C.DATE).min()
        temp[C.CLOSE] = data[name][C.CLOSE].groupby(C.DATE).nth(-1)

        # TODO:
        # volume need to be change for forex, etc cuz gives volume of -1
        # because of that, summing volume will produce wrong result
        temp[C.VOLUME] = data[name][C.VOLUME].groupby(C.DATE).sum()

        if Settings.use_complete_candles_only:
            # getting all but last candle. This is done to avoid incomplete bars at runtime
            if pd.Timestamp(self.runs_at.replace(second=0, microsecond=0)) == temp.iloc[-1].name:
                temp = temp.loc[:self.runs_at].iloc[:-1]
                logger.warning(f"Last bars for {name} {self.runs_at} were cut during data prep")
            # data[name] = temp
        return temp 
            
    def _run_portfolio(self, data):
        """
        Calculate profit and loss for the strategy
        """
        logger.info("Backtester started!")
        self.engine.run(data)
        
        # prepare data for portfolio
        self.idx = self.agg_trades.priceFluctuation_dollar.index
        self.idx = pd.Index(self.idx, dtype=object)
        self.keys = [name.split(".csv")[0] for name in data.keys]
        # check to assure order of columns is the same among all dataframes. Otherwise results will be wrong
        # ! needs to be change from hardcoded buyPrice cuz can be empty
        assert all(self.keys == self.agg_trans_prices.buyPrice.columns), "self.keys are not identical among dataframes"
        
        # nan in the beg cuz of .shift while finding priceFluctuation
        # to avoid nan in the beg
        self.agg_trades.priceFluctuation_dollar.iloc[0] = 0

        self.portfolio = Portfolio(self.agg_trades.priceFluctuation_dollar, self.idx, self.keys)
        self.portfolio.run_portfolio(self.agg_trans_prices, self.agg_custom_stop)
        
        self._generate_trade_list()

    def _generate_trade_list(self):
        logger.info("Generating trade list.")
        self.trade_list = self.agg_trades.trades.copy()
        self.trade_list[C.DATE_EXIT] = self.trade_list[C.DATE_EXIT].astype(str)
        self.trade_list = self.trade_list.sort_values(by=[C.DATE_EXIT, C.DATE_ENTRY, C.SYMBOL])
        self.trade_list.reset_index(drop=True, inplace=True)
        
        # ! a work around failing dates, when buy and sell occur on the same candle -> an null row appears for entry stats
        # self.trade_list.dropna(inplace=True)
        # assign weights
        self.trade_list[C.WEIGHT] = np.nan
        weight_list = self.trade_list.Symbol.unique()

        # ! temp putting stop loss value here
        self.trade_list[C.STOP_LOSS] = np.nan

        for asset in weight_list:
            # find all entry dates for an asset
            dates = self.trade_list[self.trade_list[C.SYMBOL] ==
                                    asset][C.DATE_ENTRY]
            # save index of the dates in trade_list for further concat
            idx = self.trade_list[self.trade_list[C.SYMBOL] ==
                                asset][C.DATE_ENTRY].index
            # grab all weights for the asset on entry date
            dates_locs = np.searchsorted(self.idx, dates)
            asset_loc = self.keys.index(asset)
            # ? might have probems with scaling (probably will)
            weights = self.portfolio.weights[dates_locs, asset_loc]

            self.trade_list.loc[idx, C.WEIGHT] = weights

            # ! temp putting stop loss value here
            # self.trade_list.loc[idx, "stop_loss"] = self.agg_stop_length.loc[dates, asset].values

        # change values to display positive for short trades (instead of negative shares)
        self.trade_list[C.WEIGHT] = np.where(self.trade_list.Direction==C.LONG,
                                    self.trade_list[C.WEIGHT], -self.trade_list[C.WEIGHT])

        # $ change
        self.trade_list[C.DOLLAR_CHANGE] = self.trade_list[C.EXIT_PRICE] - self.trade_list[C.ENTRY_PRICE]

        # % change
        self.trade_list[C.PCT_CHANGE] = (self.trade_list[C.EXIT_PRICE] -
                                        self.trade_list[C.ENTRY_PRICE]) / self.trade_list[C.ENTRY_PRICE]

        # $ profit
        self.trade_list[C.DOLLAR_PROFIT] = self.trade_list[C.WEIGHT] * self.trade_list[C.DOLLAR_CHANGE]
        self.trade_list[C.DOLLAR_PROFIT] = np.where(self.trade_list.Direction==C.LONG,
                                        self.trade_list[C.DOLLAR_PROFIT], -self.trade_list[C.DOLLAR_PROFIT])
        
        # % profit
        self.trade_list[C.PCT_PROFIT] = np.where(self.trade_list.Direction==C.LONG,
                                        self.trade_list[C.PCT_CHANGE], -self.trade_list[C.PCT_CHANGE])
        # cum profit
        self.trade_list[C.CUM_PROFIT] = self.trade_list[C.DOLLAR_PROFIT].cumsum()

        # Port value
        self.trade_list[C.PORTFOLIO_VALUE] = self.trade_list[C.DOLLAR_PROFIT].cumsum()
        self.trade_list[C.PORTFOLIO_VALUE] += Settings.start_amount

        # Position value
        self.trade_list[C.POSITION_VALUE] = self.trade_list[C.WEIGHT] * self.trade_list[C.ENTRY_PRICE]

        # number of bars held
        temp = pd.to_datetime(self.trade_list[C.DATE_EXIT], errors="coerce")
        self.trade_list[C.TRADE_DURATION] = temp - self.trade_list[C.DATE_ENTRY]
        self.trade_list[C.TRADE_DURATION] = self.trade_list[C.TRADE_DURATION].fillna("Open")

        

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
        def logic(self, current_asset):
            
            sma5 = SMA(current_asset, [C.CLOSE], 5)
            sma25 = SMA(current_asset, [C.CLOSE], 25)

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
