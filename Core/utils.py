import pandas as pd
import os
import database_stuff as db

path = r"D:\Common\Default\Documents\Projects\bt_plat\stock_data"


def csv_to_db(path):
    for ticker in os.listdir(path)[:2]:
        table_name = ticker.split(".")[0]
        df = pd.read_csv(os.path.join(path, ticker))
        con, meta = db.connect('postgres', 'undead2018', 'tennis2')
        df.to_sql(table_name, con, index=False, index_label="Date")


csv_to_db(path)