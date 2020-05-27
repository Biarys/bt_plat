import os
import pandas as pd
from functools import wraps

# own files
from . import database_stuff as db
from . import config
from . import Settings

# TODO: add support for csv/db
class DataReader:
    """
    file_type : hdf, csv, csv_files, at (automated_trading)
    If file_type == at, then submit app.data as path
    If file_type == at, then data is stored in self.path (locally)
    """
    def __init__(self, file_type, path):        
        self.data = {}
        self.type = file_type.lower()
        self.keys = None
        self.path = path

        if self.type == "hdf":
            self.get_hdf_keys()
        elif self.type == "csv":
            self.get_csv_key()
        elif self.type == "csv_files":
            self.get_csv_keys()

    def read_data(self, stock):
        if self.type == "hdf":
            return self.read_hdf(self.path, stock)
        elif self.type == "csv":
            return self.readCSV(self.path)
        elif self.type == "csv_files":
            return self.readCSVFiles(self.path, stock)
        elif self.type == "at":
            return self.path[stock]
        
    def get_csv_key(self):
        assert os.path.isfile(self.path), "You need to specify a file or the path doesnt exist."
        self.keys = [os.path.basename(self.path).split(".")[0]]
    
    def get_csv_keys(self):
        assert os.path.isdir(self.path), "You need to specify a folder or the path doesnt exist."
        self.keys = os.listdir(self.path)

    def get_hdf_keys(self):
        import h5py
        data = h5py.File(self.path, "r")
        self.keys = list(data.keys())
    
    def readCSV(self, path):
        _temp = pd.read_csv(
            path,
            index_col="Date",
        )
        _temp.index = pd.to_datetime(_temp.index)
        return _temp
    
    @staticmethod
    def readCSVFiles(path, file):
        _fileName = file.split(".csv")[0]
        _temp = pd.read_csv(
            path + "\\" + file, index_col="DateTime")
        _temp.index.name = "DateTime"
        _temp.index = pd.to_datetime(_temp.index)
        return (_fileName, _temp)

    def readDB(self, con, meta, index_col):
        """
        Reads tables from database that start with data_.
        If index_col is not provided, default name "Date" is used for index.
        Index is converted to pd.to_datetime(), so it's important to provide one.
        """
        con, meta = db.connect(config.user, config.password, config.db)
        meta.reflect(bind=con)

        for table in meta.tables.keys():
            if table.startswith("data_"):
                _temp = pd.read_sql_table(table, con, index_col=index_col)
                _temp.index.name = "Date"
                _temp.index = pd.to_datetime(_temp.index)
                self.data[table] = _temp

        # con.close()
    # add conditional decorator
    def establish_con(func):
        if Settings.read_from.lower()=="db":
            con, meta = db.connect(config.user, config.password, config.db)
            meta.reflect(bind=con)

            # @wraps(func)
            def inner(self, *args, **kwargs):
                return func(self, con, *args, **kwargs)

            # con.close() # engine closes connection automatically?
            return inner
    
    @establish_con
    def execQuery(self, con, query):
        result = pd.read_sql(query, con)
        return result

    # def read_hdf_pd(self, path):
    #     import h5py
    #     data = h5py.File(path, "r")
    #     stocks = list(data.keys())
    #     for stock in stocks:
    #         self.data[stock] = pd.read_hdf(path, stock)

    @staticmethod
    def read_hdf(path, stock):
        return (stock, pd.read_hdf(path, stock))


if __name__ == "__main__":
    test = DataReader()
    df = test.execQuery("Select * from backtests limit 10")
    print(df)
