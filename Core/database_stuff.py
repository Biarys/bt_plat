# from sqlalchemy import create_engine

# engine = create_engine('postgresql://postgres:undead2018@localhost:5432/postgres')

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import sqlalchemy_utils as sqlu
import config


def connect(user, password, db, host='localhost', port=5432):
    '''Returns connection, metadata, and session objects'''
    # We connect with the help of the PostgreSQL URL
    # postgresql://federer:grandestslam@localhost:5432/tennis
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)

    # The return value of create_engine() is our connection object
    engine = create_engine(url, client_encoding='utf8')

    Session = sessionmaker(bind=engine)
    session = Session()

    con = engine.connect()

    if sqlu.database_exists(url):
        # We then bind the connection to MetaData()
        meta = MetaData()
        # meta.reflect(bind=con)
        # print(in meta.tables)
        create_tables(engine, meta)

        return con, meta, session

    else:
        print("============================================")
        print("Database {} didn't exist".format(db))
        print("Creating database with schemas")
        print("============================================")
        sqlu.create_database(url)
        print("Database {} and schemas have been created".format(db))


def create_tables(con, meta):
    '''Create default tables'''
    backtest = Table("backtest", meta,
                     Column("backtest_id", Integer, primary_key=True),
                     Column("name", String(200)))
    backtest_trades = Table(
        "backtest_trades", meta,
        Column("backtest_id", Integer, ForeignKey("backtest.backtest_id")),
        Column("trade_id", Integer, primary_key=True),
        Column("ticker", String(20)), Column("direction", String(20)),
        Column("entry", DateTime), Column("exit", DateTime),
        Column("pct_change", String(20)), Column("profit", Float(precision=2)),
        Column("lot_size", Float(precision=8)),
        Column("position_value", Float(precision=2)),
        Column("cum_profit", Float(precision=2)), Column("bars_held", Integer))

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
    con, meta, session = connect('postgres', 'undead2018', 'tennis2')

    print(dir(con))
    print(meta)