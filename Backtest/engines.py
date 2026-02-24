from abc import ABC, abstractmethod
import logging
import pandas as pd

from Backtest.settings import Settings
from Backtest.processing import Repeater, TradeSignal, Trades, TransPrice, TransPrice, TradeSignal, Cond
from Backtest.utils import _aggregate, _find_df, _prep_and_agg_custom_stops
from Backtest import constants as C

# import pyspark.pandas as ps


logger = logging.getLogger(__name__)

class Engine(ABC):
    """
    Result of run is to process data for individual assets and save them into common aggregate classes
    """
    def __init__(self, backtest_instance):
        self.bt = backtest_instance
        logger.info(f"{self.__class__.__name__} initialized from engine.")

    @abstractmethod
    def run(self, data):
        pass

class PandasEngine(Engine):
    def run(self, data): # results needs to be aggregated
        try:
            logger.info("Running backtest with PandasEngine.")
            # ! add break condition before loop so dont waste time reading data
            for name in data.keys:
                _current_asset_tuple = data.read_data(name)
                if self.bt.preprocessing(_current_asset_tuple) == "break": # in case prepricessing is just pass
                    break
                else:
                    self.bt.preprocessing(_current_asset_tuple)
                
            # ? can implement caching. why same data is read twice?
            for name in data.keys:
                logger.info(f"Processing {name} with PandasEngine.")
                _current_asset_tuple = data.read_data(name)
                self._processing(_current_asset_tuple)
        except Exception as e:
            logger.exception(f"Error in PandasEngine run: {e}")

    def _processing(self, data): #for single asset
        """
        Loop through files
        Generate signals
        Find transaction prices
        Match buys and sells
        Save them into common classes agg_*
        """    
        try:                                            
            name = data[0]
            current_asset = data[1]
            logger.debug(f"Generating signals for {name}.")
            
            self.bt.cond = Cond()
            # strategy logic
            self.bt.logic(current_asset) # just sets buy and sell conds
            self.bt.postprocessing(current_asset)
            self.bt.cond.buy.name, self.bt.cond.sell.name, self.bt.cond.short.name, self.bt.cond.cover.name = [C.BUY, C.SELL, C.SHORT, C.COVER]
            self.bt.cond._combine() # combine all conds into all
            ################################

            rep = Repeater(current_asset, name, self.bt.cond.all_)

            # find trade_signals and trans_prices for an asset
            trade_signals = TradeSignal()
            trade_signals.run(rep)
            trans_prices = TransPrice()
            trans_prices.run(rep, trade_signals)
            trades_current_asset = Trades()
            trades_current_asset.run(rep, trade_signals, trans_prices)

            # save trans_prices for portfolio level
            self.bt.agg_trans_prices.buyPrice = _aggregate(self.bt.agg_trans_prices.buyPrice, trans_prices.buyPrice)
            self.bt.agg_trans_prices.sellPrice = _aggregate(self.bt.agg_trans_prices.sellPrice, trans_prices.sellPrice)
            self.bt.agg_trans_prices.shortPrice = _aggregate(self.bt.agg_trans_prices.shortPrice, trans_prices.shortPrice)
            self.bt.agg_trans_prices.coverPrice = _aggregate(self.bt.agg_trans_prices.coverPrice, trans_prices.coverPrice)
            self.bt.agg_trades.priceFluctuation_dollar = _aggregate(self.bt.agg_trades.priceFluctuation_dollar,
                                                                    trades_current_asset.priceFluctuation_dollar)
            self.bt.agg_trades.trades = _aggregate(self.bt.agg_trades.trades, trades_current_asset.trades, ax=0)
            self.bt.agg_stop_length = _aggregate(self.bt.agg_stop_length, self.bt.stop_length)

            # save custom stops
            if Settings.position_size_type == "custom":
                self.bt.agg_custom_stop =  _prep_and_agg_custom_stops(self.bt.agg_custom_stop, self.bt.custom_stop_size, name)
        except Exception as e:
            logger.exception(f"Error in PandasEngine _processing for {name}: {e}")

class SparkEngine(Engine):
    def run(self, data):
        # initialize spark
        # sc = pyspark.SparkContext('local[*]')
        # spark = SparkSession(sc)
        # sqlContext = SQLContext(sc)

        # read data
        # rdd = sc.parallelize(data.keys).map(data.read_data) # change to Flume (kafka not supported in python)/something more flexible
        psdf = ps.read_csv(Settings.read_from_csv_path)

        # run self.preprocessing. use collect to force action to save files
        if self.bt.preprocessing != "break":
            rdd.map(self.bt.preprocessing).collect()

        res = rdd.flatMap(self._processing)
        res_reduced = res.reduceByKey(_aggregate).collect()

        self.bt.agg_trans_prices.buyPrice = _find_df(res_reduced, "buy_price")
        self.bt.agg_trans_prices.sellPrice = _find_df(res_reduced, "sell_price")
        self.bt.agg_trans_prices.shortPrice = _find_df(res_reduced, "short_price")
        self.bt.agg_trans_prices.coverPrice = _find_df(res_reduced, "cover_price")
        self.bt.agg_trades.priceFluctuation_dollar = _find_df(res_reduced, "price_fluc_dollar")
        self.bt.agg_trades.trades = _find_df(res_reduced, "trades").T # need to transpose the result

    def _processing(self, data):
        """
        Loop through files
        Generate signals
        Find transaction prices
        Match buys and sells
        Save them into common classes agg_*
        """
        name = data[0]
        current_asset = data[1]
        logger.debug(f"Generating signals for {name}.")
        try:
            # strategy logic
            self.cond = Cond()
            self.bt.logic(current_asset)
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
            logger.error(f"Failed for {name}", exc_info=True)


# # TODO: replace with a function
# if Settings.generate_ranks:
#     rdd_p = sqlContext.read.parquet(Settings.save_temp_parquet + r"\value_*.parquet")
#     result = (rdd_p
#                 .select(
#                     'DateTime',
#                     'Symbol',
#                     pySqlFunc.rank().over(Window().partitionBy('DateTime').orderBy('Close')).alias('rank')
#                 ))
#     if Settings.order_ranks_desc:
#         result_desc = (result
#                         .select(
#                             'DateTime',
#                             'Symbol',
#                             pySqlFunc.rank().over(Window().partitionBy('DateTime').orderBy('rank')).alias('rank_desc')
#                         ))
#         result_final = (result_desc
#                         .groupby('DateTime')
#                         .pivot('Symbol')
#                         .agg(pySqlFunc.first('rank_desc'))
#                         .orderBy(pySqlFunc.col('DateTime').asc())
#                         )
#     else:
#         result_final = (result
#                         .groupby('DateTime')
#                         .pivot('Symbol')
#                         .agg(pySqlFunc.first('rank_desc'))
#                         .orderBy(pySqlFunc.col('DateTime').asc())
#                         )
    
#     result_final.toPandas().to_csv(Settings.save_temp_parquet + "\\" + Settings.rank_file_name)
