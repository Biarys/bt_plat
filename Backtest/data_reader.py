from abc import ABC, abstractmethod
import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)

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
