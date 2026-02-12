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
from auto_trading.log import setup_log
from Backtest.Settings import Settings
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
    

setup_log("Backtester")
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
        self.log = logging.getLogger("Backtester")
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
        return "break"

    def postprocessing(self, data):
        """
        Called right after logic.
        Should be used to generated values that depend on buy/sell/short/cover signals such as stops/take profits.

        Inputs: self + all data that will be used for the backtest
        """
        pass

    def run(self, data):
        try:   
            self.runs_at = dt.now()
            if self.real_time:
                for name in data.data:
                    data.data[name] = self._prepare_data(data.data, name)

            self._run_portfolio(data)
        except Exception as e:
            # print(e)
            # traceback.print_exc()
            self.log.error(e, stack_info=True)
            
    def logic(self, current_asset, name=None):
        pass

    def _prepare_data(self, data, name):
        self.log.info(f"Preparing data for {name} - {self.runs_at}")
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
                self.log.warning(f"Last bars for {name} {self.runs_at} were cut during data prep")
            # data[name] = temp
        return temp 
            

    def _prepricing_spark(self, data):
        """
        Loop through files
        Generate signals
        Find transaction prices
        Match buys and sells
        Save them into common classes agg_*
        """
        name = data[0]
        current_asset = data[1]
        try:
            # strategy logic
            self.cond = Cond()
            self.logic(current_asset, name)
            self.postprocessing(current_asset)
            self.cond.buy.name, self.cond.sell.name, self.cond.short.name, self.cond.cover.name = [C.BUY, C.SELL, C.SHORT, C.COVER]
            self.cond._combine() # combine all conds into all
            ################################
            
            rep = Repeater(current_asset, name, self.cond.all)

            # find trade_signals and trans_prices for an asset
            trade_signals = TradeSignal(rep)
            trans_prices = TransPrice(rep, trade_signals)
            trades_current_asset = Trades(rep, trade_signals, trans_prices)
            
            return (C.ENTRY_PRICE, trans_prices.buyPrice), (C.EXIT_PRICE,trans_prices.sellPrice), ("short_price", trans_prices.shortPrice), ("cover_price", trans_prices.coverPrice), \
                    ("price_fluc_dollar", trades_current_asset.priceFluctuation_dollar), ("trades", trades_current_asset.trades.T)
        except Exception as e:
            print(f"Failed for {name}")
            print(e)

    def _prepricing_pd(self, data):
        """
        Loop through files
        Generate signals
        Find transaction prices
        Match buys and sells
        Save them into common classes agg_*
        """                                                
        name = data[0]
        current_asset = data[1]
        
        self.cond = Cond()
        # strategy logic
        self.logic(current_asset, name)
        self.postprocessing(current_asset)
        self.cond.buy.name, self.cond.sell.name, self.cond.short.name, self.cond.cover.name = [C.BUY, C.SELL, C.SHORT, C.COVER]
        self.cond._combine() # combine all conds into all
        ################################

        rep = Repeater(current_asset, name, self.cond.all)

        # find trade_signals and trans_prices for an asset
        trade_signals = TradeSignal(rep)
        trans_prices = TransPrice(rep, trade_signals)
        trades_current_asset = Trades(rep, trade_signals, trans_prices)

        # save trans_prices for portfolio level
        self.agg_trans_prices.buyPrice = _aggregate(self.agg_trans_prices.buyPrice, trans_prices.buyPrice)
        self.agg_trans_prices.sellPrice = _aggregate(self.agg_trans_prices.sellPrice, trans_prices.sellPrice)
        self.agg_trans_prices.shortPrice = _aggregate(self.agg_trans_prices.shortPrice, trans_prices.shortPrice)
        self.agg_trans_prices.coverPrice = _aggregate(self.agg_trans_prices.coverPrice, trans_prices.coverPrice)
        self.agg_trades.priceFluctuation_dollar = _aggregate(self.agg_trades.priceFluctuation_dollar,
                                                                trades_current_asset.priceFluctuation_dollar)
        self.agg_trades.trades = _aggregate(self.agg_trades.trades, trades_current_asset.trades, ax=0)
        self.agg_stop_length = _aggregate(self.agg_stop_length, self.stop_length)

        # save custom stops
        if Settings.position_size_type == "custom":
            self.agg_custom_stop =  _prep_and_agg_custom_stops(self.agg_custom_stop, self.custom_stop_size, name)

    def _run_portfolio(self, data):
        """
        Calculate profit and loss for the strategy
        """
        self.log.info("Backtester started!")
        self.engine.run(data)
        
        # prepare data for portfolio
        self.idx = self.agg_trades.priceFluctuation_dollar.index
        self.idx = pd.Index(self.idx, dtype=object)
        self.keys = [name.split(".csv")[0] for name in data.keys]
        # check to assure order of columns is the same among all dataframes. Otherwise results will be wrong
        assert all(self.keys == self.agg_trans_prices.buyPrice.columns), "self.keys are not identical among dataframes"
        
        # nan in the beg cuz of .shift while finding priceFluctuation
        # to avoid nan in the beg
        self.agg_trades.priceFluctuation_dollar.iloc[0] = 0

        self.portfolio = Portfolio(self.agg_trades.priceFluctuation_dollar, self.idx, self.keys)
        self.portfolio.run_portfolio(self.agg_trans_prices, self.agg_custom_stop)
        
        self._generate_trade_list()

    def _generate_trade_list(self):
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
