import os
import pandas as pd
import database_stuff as db
import config
from functools import wraps


class DataReader:
    def __init__(self):
        self.data = {}

    def csvFile(self, path):
        assert os.path.isfile(
            path), "You need to specify a file or the path doesnt exist."
        self.data = pd.read_excel(
            path,
            index_col="Date",
            nrows=100,
            names=["Open", "High", "Low", "Close", "Volume"],
        )

    def readFiles(self, path):
        assert os.path.isdir(
            path), "You need to specify a folder or the path doesnt exist."
        for file in os.listdir(path)[:2]:
            self._fileName = file.split(".txt")[0]
            _temp = pd.read_csv(
                path + "\\" + file, nrows=100, index_col="Date/Time")
            _temp.index.name = "Date"
            _temp.index = pd.to_datetime(_temp.index)
            self.data[self._fileName] = _temp

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

        con.close()

    def establish_con(func):

        con, meta = db.connect(config.user, config.password, config.db)
        meta.reflect(bind=con)

        # @wraps(func)
        def inner(self, *args, **kwargs):
            return func(self, con, *args, **kwargs)

        con.close()
        return inner

    @establish_con
    def execQuery(self, con, query):
        # con, meta = db.connect(config.user, config.password, config.db)
        # meta.reflect(bind=con)
        # print(con)
        result = pd.read_sql(query, con)

        # con.close()
        return result


if __name__ == "__main__":
    test = DataReader()
    df = test.execQuery("Select * from backtest limit 10")
    print(df)
