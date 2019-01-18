#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import abc
import os


# In[26]:


class DataReader:
    def __init__(self):
        self.data = {}
    def csvFile(self, path):
        assert os.path.isfile(path), "You need to specify a file."
        self.data = pd.read_excel(path, index_col="Date", nrows=100, 
                   names=["Open", "High", "Low", "Close", "Volume"])
    def readFiles(self, path):
        assert os.path.isdir(path), "You need to specify a folder."
        for file in os.listdir(path)[:2]:
            self._fileName = file.split(".txt")[0]
            self.data[self._fileName] = pd.read_csv(path+file, nrows=100)


# In[27]:


data = DataReader()


# In[28]:


data.readFiles("D:/AmiBackupeSignal/")


# In[29]:


data.data


# In[2]:


# read data
data = pd.read_excel("D:/Windows/Default/Desktop/test.xlsx", index_col="Date", nrows=100, 
                   names=["Open", "High", "Low", "Close", "Volume"])
data1 = pd.read_csv("D:/AmiBackupeSignal/AABA.txt", index_col="Date/Time")
data2 = pd.read_csv("D:/AmiBackupeSignal/AAL.txt", index_col="Date/Time")


# In[3]:


data.head()


# In[30]:


class Indicator(metaclass=abc.ABCMeta):
    """Abstract class for an indicator. 
    Requires cols (of data) to be used for calculations"""
    def __init__(self, cols):
        pass
    
    @abc.abstractmethod
    def __call__(self):
        pass


# In[31]:


class SMA(Indicator):
    """Implementation of Simple Moving Average"""
    def __init__(self, cols, period):
        self.data = data[cols]
        self.period = period
        
    def __call__(self):
        self.result = self.data.rolling(self.period).mean()
        # fillna cuz NaNs result from mean() are strings
        self.result.fillna(np.NaN, inplace=True)
        # need to convert dataframe to series for comparison with series
        return pd.Series(self.result["Close"], self.result.index)


# In[32]:


sma5 = SMA(["Close"], 5)


# In[7]:


sma25 = SMA(["Close"], 25)


# In[8]:


buyCond = sma5() > sma25()
sellCond = sma5() < sma25()
# generates trade signal
# compares current cond signal with itself shifted to see the change from true to false
# .shift(1) in the end to avoid premature buy
#tradeSignal = cond.where(cond != cond.shift(1).fillna(cond[0])).shift(1)


# In[9]:


class TradeSignal:
    """
    For now, long only. 1 where buy, 0 where sell
    Possibly add signal shift - added for now to match excel results
    """
    # not using self because the need to pass buy and sell cond
    #buyCond = buyCond.where(buyCond != buyCond.shift(1).fillna(buyCond[0])).shift(1)
    #sellCond = sellCond.where(sellCond != sellCond.shift(1).fillna(sellCond[0])).shift(1)
    def __init__(self, buyCond, sellCond):
        # buy/sell/all signals
        self.buyCond = buyCond.where(buyCond != buyCond.shift(1).fillna(buyCond[0])).shift(1)
        self.sellCond = sellCond.where(sellCond != sellCond.shift(1).fillna(sellCond[0])).shift(1)
        # self.tradeSignals = cond.where(cond != cond.shift(1).fillna(cond[0])).shift(1)
        pass


# In[10]:


ts = TradeSignal(buyCond, sellCond)


# In[11]:


# ts.sellCond[ts.sellCond == 1] = "Sell"


# In[12]:


class TransPrice(TradeSignal):
    """
    Raw transaction price meaning only initial buy and sellf prices are recorded without forward fill.
    """
    def __init__(self, buyOn="Close", sellOn="Close"):
        # buy price & sell price
        super().__init__(buyCond, sellCond)
        self.buyPrice = data[buyOn][self.buyCond == 1]
        self.sellPrice = data[sellOn][self.sellCond == 1]
        cond = [
            (self.buyCond == 1),
            (self.sellCond == 1)
        ]
        out = ["Buy", "Sell"]
        self.test = np.select(cond, out)
        self.test = pd.DataFrame(self.test, index=data.index)
        #self.test.fill("0", np.NAN)


# In[13]:


tp = TransPrice()


# In[14]:


tp.test[tp.test != "0"].dropna()


# In[15]:


class Returns(TransPrice):
    def __init__(self):
        tp = TransPrice()
        self.index = data.index
        self.returns = pd.DataFrame(index=self.index, columns=["Returns"])
        # might result in errors tradesignal/execution is shifted is shifted
        self.returns["Returns"].loc[tp.buyPrice.index] = tp.buyPrice
        self.returns["Returns"].loc[tp.sellPrice.index] = tp.sellPrice
        self.returns = self.returns.dropna().pct_change()
        #works for now
        for i in self.returns.index:
            if tp.test.loc[i][0] == "Buy":
                self.returns.loc[i] = -self.returns.loc[i]
        #self.returns.ffill(inplace=True)


# In[16]:


ret = Returns()


# In[17]:


ret.returns


# In[18]:


class Stats():
    def __init__(self):
        r = Returns()
        self.posReturns = r.returns[r.returns > 0].dropna()
        self.negReturns = r.returns[r.returns < 0].dropna()
        self.posTrades = len(self.posReturns)
        self.negTrades = len(self.negReturns)
        self.meanReturns = r.returns.mean()
        self.hitRatio = self.posTrades/(self.posTrades+self.negTrades)
        self.totalTrades = self.posTrades+self.negTrades


# In[19]:


stats = Stats()


# In[22]:


stats.negReturns


# In[ ]:


os.

