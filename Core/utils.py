import pandas as pd
import os

# own files
from . import database_stuff as db
from . import config

path = r"D:\Common\Default\Documents\Projects\bt_plat\stock_data"


def csv_to_db(path):
    con, meta, session = db.connect(config.user, config.password, config.db)

    for ticker in os.listdir(path)[:2]:
        table_name = "data_" + ticker.split(".")[0]
        df = pd.read_csv(os.path.join(path, ticker))
        df["Date"] = pd.to_datetime(df["Date"])
        try:
            df.to_sql(
                table_name,
                con,
                index=False,
                index_label="Date",
                if_exists="replace")
            con.execute(
                'ALTER TABLE "{}" ADD PRIMARY KEY ("Date")'.format(table_name))
        except Exception as e:
            print(e)


csv_to_db(path)