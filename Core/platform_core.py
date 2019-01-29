import pandas as pd
import numpy as np
import abc
import os

class DataReader:
    """
    Reads data from files and stores them in a dict.
    Need to add support for databases.
    Need to add buffer.
    """
    def __init__(self):
        """
        Initialize out dict.
        """
        self.data = {}

    def csvFile(self, path):
        """
        Read single file
        """
        assert os.path.isfile(path), "You need to specify a file."
        self.data = pd.read_excel(path, index_col="Date", nrows=100,
                   names=["Open", "High", "Low", "Close", "Volume"])

    def readFiles(self, path):
        """
        Read multiple files form a folder
        """
        assert os.path.isdir(path), "You need to specify a folder."
        for file in os.listdir(path)[:2]:
            self._fileName = file.split(".txt")[0]
            self.data[self._fileName] = pd.read_csv(path+file, nrows=100)

data = DataReader()

data.readFiles("D:/AmiBackupeSignal/")

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
    """
    Implementation of Simple Moving Average
    """
    def __init__(self, ts, cols, period):
        self.data = ts[cols]
        self.period = period

    def __call__(self):
        self.result = self.data.rolling(self.period).mean()
        # fillna cuz NaNs result from mean() are strings
        self.result.fillna(np.NaN, inplace=True)
        # need to convert dataframe to series for comparison with series
        return pd.Series(self.result["Close"], self.result.index)

#buyCond = sma5() > sma25()
#sellCond = sma5() < sma25()
# generates trade signal
# compares current cond signal with itself shifted to see the change from true to false
# .shift(1) in the end to avoid premature buy
#tradeSignal = cond.where(cond != cond.shift(1).fillna(cond[0])).shift(1)

class TradeSignal:
    """
    For now, long only. 1 where buy, 0 where sell
    Possibly add signal shift - added for now to match excel results
    """
    # not using self because the need to pass buy and sell cond
    # buyCond = buyCond.where(buyCond != buyCond.shift(1).fillna(buyCond[0])).shift(1)
    # sellCond = sellCond.where(sellCond != sellCond.shift(1).fillna(sellCond[0])).shift(1)
    def __init__(self, buyCond, sellCond):
        # buy/sell/all signals
        self.buyCond = buyCond.where(buyCond != buyCond.shift(1).fillna(buyCond[0])).shift(1)
        self.sellCond = sellCond.where(sellCond != sellCond.shift(1).fillna(sellCond[0])).shift(1)
        # self.tradeSignals = cond.where(cond != cond.shift(1).fillna(cond[0])).shift(1)

class TransPrice(TradeSignal):
    """
    Raw transaction price meaning only initial buy and sell prices are recorded without forward fill
    """
    def __init__(self, ts, buyCond, sellCond, buyOn="Close", sellOn="Close"):
        # buy price & sell price
        super().__init__(buyCond, sellCond)
        self.buyPrice = ts[buyOn][self.buyCond == 1]
        self.sellPrice = ts[sellOn][self.sellCond == 1]
        cond = [
            (self.buyCond == 1),
            (self.sellCond == 1)
        ]
        out = ["Buy", "Sell"]
        self.test = np.select(cond, out)
        self.test = pd.DataFrame(self.test, index=ts.index)
        #self.test.fill("0", np.NAN)

class Returns(TransPrice):
    """
    Calculates returns for the strategy
    """
    def __init__(self, d, buyCond, sellCond):
        tp = TransPrice(d, buyCond, sellCond)
        self.index = d.index
        self.returns = pd.DataFrame(index=self.index, columns=["Returns"])
        # might result in errors tradesignal/execution is shifted
        self.returns["Returns"].loc[tp.buyPrice.index] = tp.buyPrice
        self.returns["Returns"].loc[tp.sellPrice.index] = tp.sellPrice
        self.returns = self.returns.dropna().pct_change()
        #works for now
        for i in self.returns.index:
            if tp.test.loc[i][0] == "Buy":
                self.returns.loc[i] = -self.returns.loc[i]
        #self.returns.ffill(inplace=True)

class Stats:
    """
    Calculats various trade statistics based on returns
    """
    def __init__(self, d, buyCond, sellCond):
        r = Returns(d, buyCond, sellCond)
        self.posReturns = r.returns[r.returns > 0].dropna()
        self.negReturns = r.returns[r.returns < 0].dropna()
        self.posTrades = len(self.posReturns)
        self.negTrades = len(self.negReturns)
        self.meanReturns = r.returns.mean()
        self.hitRatio = self.posTrades/(self.posTrades+self.negTrades)
        self.totalTrades = self.posTrades+self.negTrades

def run():
    for i in data.data:
        d = data.data[i]
        sma5 = SMA(d, ["Close"], 5)
        sma25 = SMA(d, ["Close"], 25)
        buyCond = sma5() > sma25()
        sellCond = sma5() < sma25()
        ts = TradeSignal(buyCond, sellCond)
        print(ts.buyCond)
        tp = TransPrice(d, buyCond, sellCond)
        print(tp.buyPrice)
        ret = Returns(d, buyCond, sellCond)
        print(ret.returns)
        stats = Stats(d, buyCond, sellCond)

run()
