import abc
import numpy as np
import pandas as pd

#############################################
# Indicators
#############################################


class Indicator(metaclass=abc.ABCMeta):
    """
    Abstract class for an indicator. 
    Requires cols (of data) to be used for calculations
    """

    def __init__(self, cols):
        pass

    @abc.abstractmethod
    def __call__(self):
        pass


class SMA(Indicator):
    """Implementation of Simple Moving Average"""

    def __init__(self, time_series, cols, period):
        self.data = time_series[cols]
        self.period = period

    def __call__(self):
        self.result = self.data.rolling(self.period).mean()
        # fillna cuz NaNs result from mean() are strings
        self.result.fillna(np.NaN, inplace=True)
        # need to convert dataframe to series for comparison with series
        return pd.Series(self.result["Close"], self.result.index)
