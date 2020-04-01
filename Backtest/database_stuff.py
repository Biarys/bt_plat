#########################################
# Creates connection, database and tables
#########################################

# from sqlalchemy import create_engine

# engine = create_engine('postgresql://postgres:undead2018@localhost:5433/postgres')

import sqlalchemy_utils as sqlu
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, ForeignKey, DateTime

# TODO: needs to be moved/changed
import os
# if not os.path.exists("Backtest/config.py"):
#     with open("Backtest/config.py", "w+") as config_file:
#         config_file.write("user='TestUser' \npassword='TestPass'\ndb='testDB'")
# own files
from . import config


def connect(user, password, db, host='localhost', port=5432):
    '''Returns connection, metadata, and session objects'''
    # We connect with the help of the PostgreSQL URL
    # postgresql://federer:grandestslam@localhost:5432/tennis
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)

    # The return value of create_engine() is our connection object
    engine = create_engine(url, client_encoding='utf8')

    # Session = sessionmaker(bind=engine)
    # session = Session()

    # con = engine.connect()

    if sqlu.database_exists(url):
        # We then bind the connection to MetaData()
        meta = MetaData()
        # meta.reflect(bind=con)
        # print(in meta.tables)
        create_tables(engine, meta)

        # passing engine instead of connection because deal with raw SQL
        return engine, meta  # , session

    else:
        print("============================================")
        print("Database {} didn't exist".format(db))
        print("Creating database with schemas")
        print("============================================")
        sqlu.create_database(url)
        print("Database {} and schemas have been created".format(db))


def create_tables(con, meta):
    '''Create default tables'''
    backtests = Table("backtests", meta,
                      Column("backtest_id", Integer, primary_key=True),
                      Column("name", String(200)))
    trades = Table(
        "trades", meta,
        Column("backtest_id", Integer, ForeignKey("backtests.backtest_id")),
        Column("trade_id", Integer, primary_key=True),
        Column("ticker", String(20)), Column("direction", String(20)),
        Column("entry", DateTime), Column("exit", DateTime),
        Column("pct_change", String(20)), Column("profit", Float(precision=2)),
        Column("lot_size", Float(precision=8)),
        Column("position_value", Float(precision=2)),
        Column("cum_profit", Float(precision=2)), Column("bars_held", Integer))
    # agg_trade_signals = Table(
    #     "trade_signals", meta,
    #     Column("backtest_id", Integer, ForeignKey("backtests.backtest_id")),
    #     Column("asset", String(20)),
    #     Column()
    # )
    # agg_trans_price =

    # Column("CAGR", Float(precision=2)),
    # Column("maxDD", Float(precision=2)),
    # Column("initial_capital", Float(precision=2)),
    # Column("ending_capital", Float(precision=2)),
    # Column("net_profit", Float(precision=2)),
    # Column("net_profit_pct", Float(precision=2)),
    # Column("transaction_cost", Float(precision=2)),
    # Column("number_of_trades", Float(precision=2)),
    # Column("avg_profit", Float(precision=2)),
    # Column("avg_profit_pct", Float(precision=2)),
    # Column("avg_bars_held", Float(precision=2)),
    # Column("net_profit", Float(precision=2)),
    # Column("net_profit", Float(precision=2)),
    # Column("net_profit", Float(precision=2)),
    # Column("net_profit", Float(precision=2)),
    # Column()
    # Column()

    meta.create_all(con, checkfirst=True)


# Base = declarative_base()

# class StockData(Base):
#     def __init__(self, table_name, d, o, h, l, c, v, **kwargs):
#         print(table_name)
#         super().__init____(**kwargs)
#         __tablename__ = "table_name"
#         d = Column("Date", DateTime, primary_key=True)  # Date
#         o = Column("Open", Float)  # Open
#         h = Column("High", Float)  # High
#         l = Column("Low", Float)  # Low
#         c = Column("Close", Float)  # Close
#         v = Column("Volume", Integer)  # Volume

if __name__ == "__main__":
    con, meta, session = db.connect(config.user, config.password, config.db)

    print(dir(con))
    print(meta)
