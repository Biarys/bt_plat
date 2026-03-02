from abc import ABC, abstractmethod
import logging
import pandas as pd

from Backtest.settings import Settings
from Backtest.processing import Repeater, TradeSignal, Trades, TransPrice, Cond, SingleAssetResult
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
    def run(self, data):
        """
        Runs the backtest logic over all assets in data and aggregates results.
        Returns a dictionary containing the aggregated metrics.
        """
        try:
            logger.info("Running backtest with PandasEngine.")
            for name in data.keys:
                _current_asset_tuple = data.read_data(name)
                if self.bt.preprocessing(_current_asset_tuple) == "break":
                    break
                self.bt.preprocessing(_current_asset_tuple)
                
            results = []
            for name in data.keys:
                logger.info(f"Processing {name} with PandasEngine.")
                _current_asset_tuple = data.read_data(name)
                res = PandasEngine._processing(_current_asset_tuple, self.bt.logic, self.bt.postprocessing, self.bt.custom_stop_size, self.bt.stop_length)
                results.append(res)
                
            # Aggregate the stateless results
            agg_buy_price = pd.DataFrame()
            agg_sell_price = pd.DataFrame()
            agg_short_price = pd.DataFrame()
            agg_cover_price = pd.DataFrame()
            agg_price_fluctuation_dollar = pd.DataFrame()
            agg_trades = pd.DataFrame()
            agg_stop_length = pd.DataFrame()
            agg_custom_stop = pd.DataFrame()

            for res in results:
                agg_buy_price = _aggregate(agg_buy_price, res.buy_price)
                agg_sell_price = _aggregate(agg_sell_price, res.sell_price)
                agg_short_price = _aggregate(agg_short_price, res.short_price)
                agg_cover_price = _aggregate(agg_cover_price, res.cover_price)
                agg_price_fluctuation_dollar = _aggregate(agg_price_fluctuation_dollar, res.price_fluctuation_dollar)
                agg_trades = _aggregate(agg_trades, res.trades, ax=0)
                if not res.stop_length.empty:
                    agg_stop_length = _aggregate(agg_stop_length, res.stop_length)
                if not res.custom_stop.empty:
                    agg_custom_stop = _aggregate(agg_custom_stop, res.custom_stop)

            return {
                "buy_price": agg_buy_price,
                "sell_price": agg_sell_price,
                "short_price": agg_short_price,
                "cover_price": agg_cover_price,
                "price_fluctuation_dollar": agg_price_fluctuation_dollar,
                "trades": agg_trades,
                "stop_length": agg_stop_length,
                "custom_stop": agg_custom_stop
            }
        except Exception as e:
            logger.exception(f"Error in PandasEngine run: {e}")
            raise

    @staticmethod
    def _processing(data, logic_func, postprocessing_func, custom_stop_size=None, stop_length=pd.DataFrame()): #for single asset
        """
        Pure function:
        Loop through files -> Generate signals -> Find transaction prices -> Match buys and sells
        Returns a SingleAssetResult containing all generated data for the asset.
        """
        name = data[0]
        current_asset = data[1]
        logger.debug(f"Generating signals for {name}.")
        try:
            # We mock 'self' that the user's logic_func expects.
            # In a true rewrite, logic_func wouldn't need 'self' to mutate cond.
            class MockBT:
                def __init__(self):
                    self.cond = Cond()
            
            mock_bt = MockBT()
            
            # strategy logic
            logic_func.__get__(mock_bt)(current_asset)
            postprocessing_func.__get__(mock_bt)(current_asset)
            
            mock_bt.cond.buy.name, mock_bt.cond.sell.name, mock_bt.cond.short.name, mock_bt.cond.cover.name = [C.BUY, C.SELL, C.SHORT, C.COVER]
            mock_bt.cond._combine() # combine all conds into all
            ################################

            rep = Repeater(current_asset, name, mock_bt.cond.all_)

            # find trade_signals and trans_prices for an asset
            trade_signals = TradeSignal()
            trade_signals.run(rep)
            trans_prices = TransPrice()
            trans_prices.run(rep, trade_signals)
            trades_current_asset = Trades()
            trades_current_asset.run(rep, trade_signals, trans_prices)

            res = SingleAssetResult(
                name=name,
                buy_price=trans_prices.buy_price,
                sell_price=trans_prices.sell_price,
                short_price=trans_prices.short_price,
                cover_price=trans_prices.cover_price,
                price_fluctuation_dollar=trades_current_asset.price_fluctuation_dollar,
                trades=trades_current_asset.trades,
                stop_length=stop_length
            )

            if Settings.position_size_type == "custom":
                 res.custom_stop = _prep_and_agg_custom_stops(pd.DataFrame(), custom_stop_size, name)
            
            return res
        except Exception as e:
            logger.exception(f"Error in PandasEngine _processing for {name}: {e}")
            raise

class SparkEngine(Engine):
    def run(self, data):
        """
        Runs the backtest logic over all assets in data using PySpark.
        Distributes data using RDDs to reuse the pure `_processing` function.
        """
        from pyspark.sql import SparkSession
        
        try:
            logger.info("Initializing SparkSession.")
            spark = SparkSession.builder \
                .master("local[*]") \
                .appName(f"BacktestEngine_{self.bt.name}") \
                .getOrCreate()
            sc = spark.sparkContext
            
            # Since preprocessing might need to break, execute it on driver
            logger.info("Running preprocessing on driver.")
            for name in data.keys:
                _current_asset_tuple = data.read_data(name)
                if self.bt.preprocessing(_current_asset_tuple) == "break":
                    break
                self.bt.preprocessing(_current_asset_tuple)
            
            logger.info("Distributing Backtest task over Spark cluster.")
            
            # Distribute keys to cluster.
            keys_rdd = sc.parallelize(data.keys)
            
            # Define a mapper function that encapsulates reading and processing
            # note: this assumes `data` (ReaderFactory) is serializable or workers can read the paths directly
            def map_logic(name):
                # We do the read locally on the worker
                _current_asset_tuple = data.read_data(name)
                return PandasEngine._processing(
                    _current_asset_tuple,
                    self.bt.logic,
                    self.bt.postprocessing,
                    self.bt.custom_stop_size,
                    self.bt.stop_length
                )
            
            # Execute on cluster
            results = keys_rdd.map(map_logic).collect()
            
            logger.info("Aggregating Spark results on driver.")
            # Aggregate the stateless results exactly as PandasEngine does
            agg_buy_price = pd.DataFrame()
            agg_sell_price = pd.DataFrame()
            agg_short_price = pd.DataFrame()
            agg_cover_price = pd.DataFrame()
            agg_price_fluctuation_dollar = pd.DataFrame()
            agg_trades = pd.DataFrame()
            agg_stop_length = pd.DataFrame()
            agg_custom_stop = pd.DataFrame()

            for res in results:
                agg_buy_price = _aggregate(agg_buy_price, res.buy_price)
                agg_sell_price = _aggregate(agg_sell_price, res.sell_price)
                agg_short_price = _aggregate(agg_short_price, res.short_price)
                agg_cover_price = _aggregate(agg_cover_price, res.cover_price)
                agg_price_fluctuation_dollar = _aggregate(agg_price_fluctuation_dollar, res.price_fluctuation_dollar)
                agg_trades = _aggregate(agg_trades, res.trades, ax=0)
                if not res.stop_length.empty:
                    agg_stop_length = _aggregate(agg_stop_length, res.stop_length)
                if not res.custom_stop.empty:
                    agg_custom_stop = _aggregate(agg_custom_stop, res.custom_stop)

            return {
                "buy_price": agg_buy_price,
                "sell_price": agg_sell_price,
                "short_price": agg_short_price,
                "cover_price": agg_cover_price,
                "price_fluctuation_dollar": agg_price_fluctuation_dollar,
                "trades": agg_trades,
                "stop_length": agg_stop_length,
                "custom_stop": agg_custom_stop
            }
            
        except Exception as e:
            logger.exception(f"Error in SparkEngine run: {e}")
            raise

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
