from abc import ABC, abstractmethod
import pandas as pd
# import pyspark
# from pyspark.sql import SparkSession, SQLContext
# from pyspark.sql.window import Window
# import pyspark.sql.functions as pySqlFunc

from Backtest.Settings import Settings
from Backtest.utils import _aggregate, _find_df

class Engine(ABC):
    def __init__(self, backtest_instance):
        self.bt = backtest_instance

    @abstractmethod
    def run(self, data):
        pass

class PandasEngine(Engine):
    def run(self, data):
        # ! add break condition before loop so dont waste time reading data
        for name in data.keys:
            _current_asset_tuple = data.read_data(name)
            if self.bt.preprocessing(_current_asset_tuple) == "break": # in case prepricessing is just pass
                break
            else:
                self.bt.preprocessing(_current_asset_tuple)
            
        # ? can implement caching. why same data is read twice?
        for name in data.keys:    
            _current_asset_tuple = data.read_data(name)
            self.bt._prepricing_pd(_current_asset_tuple)

class SparkEngine(Engine):
    def run(self, data):
        # initialize spark
        sc = pyspark.SparkContext('local[*]')
        spark = SparkSession(sc)
        sqlContext = SQLContext(sc)

        # read data
        rdd = sc.parallelize(data.keys).map(data.read_data) # change to Flume (kafka not supported in python)/something more flexible

        # run self.preprocessing. use collect to force action to save files
        if self.bt.preprocessing != "break":
            rdd.map(self.bt.preprocessing).collect()

        # TODO: replace with a function
        if Settings.generate_ranks:
            rdd_p = sqlContext.read.parquet(Settings.save_temp_parquet + r"\value_*.parquet")
            result = (rdd_p
                        .select(
                            'DateTime',
                            'Symbol',
                            pySqlFunc.rank().over(Window().partitionBy('DateTime').orderBy('Close')).alias('rank')
                        ))
            if Settings.order_ranks_desc:
                result_desc = (result
                                .select(
                                    'DateTime',
                                    'Symbol',
                                    pySqlFunc.rank().over(Window().partitionBy('DateTime').orderBy('rank')).alias('rank_desc')
                                ))
                result_final = (result_desc
                                .groupby('DateTime')
                                .pivot('Symbol')
                                .agg(pySqlFunc.first('rank_desc'))
                                .orderBy(pySqlFunc.col('DateTime').asc())
                                )
            else:
                result_final = (result
                                .groupby('DateTime')
                                .pivot('Symbol')
                                .agg(pySqlFunc.first('rank_desc'))
                                .orderBy(pySqlFunc.col('DateTime').asc())
                                )
            
            result_final.toPandas().to_csv(Settings.save_temp_parquet + "\\" + Settings.rank_file_name)
        
        res = rdd.flatMap(self.bt._prepricing_spark)
        res_reduced = res.reduceByKey(_aggregate).collect()

        self.bt.agg_trans_prices.buyPrice = _find_df(res_reduced, "buy_price")
        self.bt.agg_trans_prices.sellPrice = _find_df(res_reduced, "sell_price")
        self.bt.agg_trans_prices.shortPrice = _find_df(res_reduced, "short_price")
        self.bt.agg_trans_prices.coverPrice = _find_df(res_reduced, "cover_price")
        self.bt.agg_trades.priceFluctuation_dollar = _find_df(res_reduced, "price_fluc_dollar")
        self.bt.agg_trades.trades = _find_df(res_reduced, "trades").T # need to transpose the result
