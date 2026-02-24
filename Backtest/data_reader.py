from abc import ABC, abstractmethod
import logging
import os
import pandas as pd

from Backtest import constants as C
from Backtest.settings import Settings

logger = logging.getLogger(__name__)


def prepare_data(data, name, runs_at):
    """
    Aggregates intraday bars into daily OHLCV candles.
    Only used in real-time trading mode.

    Parameters:
        data:     dict of DataFrames keyed by asset name
        name:     key of the asset to prepare
        runs_at:  datetime the backtest was triggered (used to trim incomplete bars)

    Returns:
        DataFrame of daily OHLCV bars
    """
    logger.info(f"Preparing data for {name} - {runs_at}")

    temp = pd.DataFrame(columns=data[name].columns)
    temp.index.name = C.DATE
    temp[C.OPEN] = data[name][C.OPEN].groupby(C.DATE).nth(0)
    temp[C.HIGH] = data[name][C.HIGH].groupby(C.DATE).max()
    temp[C.LOW] = data[name][C.LOW].groupby(C.DATE).min()
    temp[C.CLOSE] = data[name][C.CLOSE].groupby(C.DATE).nth(-1)

    # TODO: volume needs to change for forex etc (volume of -1 makes sum wrong)
    temp[C.VOLUME] = data[name][C.VOLUME].groupby(C.DATE).sum()

    if Settings.use_complete_candles_only:
        # drop the last (incomplete) bar if it matches the current run time
        if pd.Timestamp(runs_at.replace(second=0, microsecond=0)) == temp.iloc[-1].name:
            temp = temp.loc[:runs_at].iloc[:-1]
            logger.warning(f"Last bar for {name} at {runs_at} was cut (incomplete candle)")

    return temp

class BaseReader(ABC):
    def __init__(self, path):
        self.path = path
        self.keys = self.get_keys()
        logger.info(f"Initialized {self.__class__.__name__} with path: {self.path}")

    @abstractmethod
    def get_keys(self):
        pass

    @abstractmethod
    def read_data(self, key):
        pass

class HDFReader(BaseReader):
    def get_keys(self):
        import h5py
        with h5py.File(self.path, "r") as data:
            return list(data.keys())

    def read_data(self, stock):
        return (stock, pd.read_hdf(self.path, stock))

class CSVReader(BaseReader):
    def get_keys(self):
        assert os.path.isfile(self.path), "You need to specify a file or the path doesnt exist."
        return [os.path.basename(self.path).split(".")[0]]

    def read_data(self, stock):
        _temp = pd.read_csv(
            self.path,
            index_col="Date",
        )
        _temp.index = pd.to_datetime(_temp.index)
        return (stock, _temp)

class CSVFilesReader(BaseReader):
    def get_keys(self):
        assert os.path.isdir(self.path), "You need to specify a folder or the path doesnt exist."
        return os.listdir(self.path)

    def read_data(self, file):
        _fileName = file.split(".csv")[0]
        file_path = os.path.join(self.path, file)
        _temp = pd.read_csv(
            file_path, index_col="Date")
        _temp.index.name = "Date"
        _temp.index = pd.to_datetime(_temp.index)
        return (_fileName, _temp)

class ATReader(BaseReader):
    def __init__(self, data):
        self.data = data
        super().__init__(path=None)

    def get_keys(self):
        return self.data.keys()

    def read_data(self, stock):
        return (stock, self.data[stock])

class ReaderFactory:
    def __init__(self, file_type, path_or_data):
        self.reader = self._get_reader(file_type, path_or_data)
        self.keys = self.reader.keys

    def _get_reader(self, file_type, path_or_data):
        file_type = file_type.lower()
        if file_type == "hdf":
            return HDFReader(path_or_data)
        elif file_type == "csv_file":
            return CSVReader(path_or_data)
        elif file_type == "csv_files":
            return CSVFilesReader(path_or_data)
        elif file_type == "at":
            return ATReader(path_or_data)
        else:
            raise ValueError(f"Unsupported file_type: {file_type}")

    def read_data(self, key):
        return self.reader.read_data(key)
