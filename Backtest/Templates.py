import abc

class DataReader:
    def __init__(self):
        self.data = {}
    def csvFile(self, path):
        #read one _file
        pass
    def readFiles(self, path):
        #read many files
        pass

class Indicator(metaclass=abc.ABCMeta):
    """Abstract class for an indicator.
    Requires cols (of data) to be used for calculations"""
    @abc.abstractmethod
    def __init__(self, cols):
        pass

    @abc.abstractmethod
    def __call__(self):
        pass

buyCond = sma5() > sma25()
sellCond = sma5() < sma25()

class TradeSignal:
    """
    For now, long only. 1 where buy, 0 where sell
    Possibly add signal shift - added for now to match excel results
    """
    def __init__(self, buyCond, sellCond):
        pass

class TransPrice(TradeSignal):
    """
    Raw transaction price meaning only initial buy and sellf prices are recorded without forward fill.
    """
    def __init__(self, buyOn="Close", sellOn="Close"):
        pass

class Returns(TransPrice):
    """
    Calculate returns
    """
    def __init__(self):
        pass

class Stats():
    """
    Calculate stats on returns
    """
    def __init__(self):
        pass
