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
        self.result = self.data.rolling(self.period).mean().round(4)
        # fillna cuz NaNs result from mean() are strings
        self.result.fillna(np.NaN, inplace=True)
        # need to convert dataframe to series for comparison with series
        return pd.Series(self.result["Close"], self.result.index)

class ATR(Indicator):
    def __init__(self, current_asset, period):
        self.data = current_asset
        self.period = period

    def __call__(self):
        temp = pd.DataFrame()
        temp["Method1"] = self.data["High"] - self.data["Low"]
        temp["Method2"] = abs(self.data["High"] - self.data["Close"].shift())
        temp["Method3"] = abs(self.data["Low"] - self.data["Close"].shift())
        temp = temp.max(1) # get max of all 3 methods
        temp = temp.ewm(14).mean()
        return temp
