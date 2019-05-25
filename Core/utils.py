import pandas as pd
import os
import database_stuff as db

path = r"D:\Common\Default\Documents\Projects\bt_plat\stock_data"


def csv_to_db(path):
    con, meta, session = db.connect('postgres', 'undead2018', 'tennis2')

    for ticker in os.listdir(path)[:2]:
        table_name = ticker.split(".")[0]
        df = pd.read_csv(os.path.join(path, ticker))
        # print(con.execute("show search_path"))
        # print(con.execute('Select * from AA;'))

        # stock = db.StockData(
        #     table_name=table_name,
        #     d=df["Date"],
        #     o=df["Open"],
        #     h=df["High"],
        #     l=df["Low"],
        #     c=df["Close"],
        #     v=df["Volume"])
        # session.add(stock)
        # session.commit()
        df["Date"] = pd.to_datetime(df["Date"])
        df.to_sql(
            table_name,
            con,
            index=False,
            index_label="Date",
            if_exists="append")
        con.execute('ALTER TABLE public."{}" ADD PRIMARY KEY ("Date")'.format(
            table_name))


csv_to_db(path)