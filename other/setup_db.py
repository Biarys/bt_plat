# %% 
import pandas as pd
import os
from sqlalchemy import create_engine


if __name__=="__main__":
    path = r"E:\Windows\Data"
    engine = create_engine("postgresql://postgres:admin@localhost/stocks")
    
    tickers = [stock.split(".csv")[0] for stock in os.listdir(path)]
    mapping = zip(range(len(os.listdir(path))), tickers)
    maps = pd.DataFrame(mapping, columns=["id", "ticker"])
    maps.set_index("id", inplace=True)
    # maps.to_sql("mapping", con=engine)

    # for stock in os.listdir(path)[:3]:
    #     df = pd.read_csv(path+"\\"+stock, index_col="DateTime")
    #     df.index = pd.to_datetime(df.index)
    #     df["Symbol"] = maps[maps["ticker"]==stock.split(".csv")[0]].index[0]
    #     print(stock)
    #     # for year in [2020]:
    #     # for year in range(2000, 2021):  
    #     try:
    #         temp = df
    #         temp[["Open", "High", "Low", "Close"]] = temp[["Open", "High", "Low", "Close"]].round(2)
    #         temp.columns = [col.lower() for col in temp.columns]
    #         # temp = df.loc[str(year)]           
    #         temp.to_sql("all_in_one", con=engine, if_exists="append")
    #     except Exception as e:
    #         print(f"An error has occured with: {stock}, error: {e}")
    
    # with engine.connect() as con:
    #     for stock in os.listdir(path)[:3]:
    #         statement = f"COPY all_in_one FROM '{path}\{stock}' WITH (FORMAT csv);"
    #         # print(statement)
    #         con.execute(statement)

    hdf_file = r"D:\HDF5\stocks.h5"

    
    for stock in os.listdir(path):
        df = pd.read_csv(path+"\\"+stock, index_col="DateTime")
        df.index = pd.to_datetime(df.index)
        #df["Symbol"] = maps[maps["ticker"]==stock.split(".csv")[0]].index[0]
        print(stock)
        stock_name = stock.split(".csv")[0]
        df[["Open", "High", "Low", "Close"]] = df[["Open", "High", "Low", "Close"]].round(2)
        df.to_hdf(hdf_file, stock_name, mode="a", format="table")

    # df = pd.read_hdf(hdf_file)

    # print(df)