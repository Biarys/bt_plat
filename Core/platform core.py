import pandas as pd
import numpy as np
import os
import abc

# own files
import data_reader
from indicators import *
import database_stuff as db
import config

#############################################
# Data reading
# Construct indicator
# Generate signals
# Find entries and exits
# Calculate portfolio value
# Generate portfolio statistics
#############################################

#############################################
# Read data
#############################################

# data = data_reader.DataReader()
# data.readFiles(r"D:\AmiBackupeSignal")

con, meta, session = db.connect(config.user, config.password, config.db)
meta.reflect(bind=con)

data = data_reader.DataReader()
data.readDB(con, meta, index_col="Date")

# print(data.data["data_AAPL"].head())


#############################################
# Core starts
#############################################
class Backtest:
    def __init__(self, name):
        self.name = name
        self.id = con.execute(
            "INSERT INTO \"backtests\" (name) VALUES ('{}') RETURNING backtest_id"
            .format(self.name)).fetchall()[0][0]  # fetchall() to get the tuple

        print(f"Backtest #{self.id} is running")


b = Backtest("test")


class TradeSignal:
    """
    Find trade signals for current asset
    For now, long only. 1 where buy, 0 where sell
    Possibly add signal shift - added for now to match excel results
    """

    # not using self because the need to pass buy and sell cond
    # buyCond = buyCond.where(buyCond != buyCond.shift(1).fillna(buyCond[0])).shift(1)
    # sellCond = sellCond.where(sellCond != sellCond.shift(1).fillna(sellCond[0])).shift(1)

    def __init__(self, rep):
        # rep = rep
        # buy/sell/all signals
        self.buyCond = rep.buyCond.where(
            rep.buyCond != rep.buyCond.shift(1).fillna(rep.buyCond[0])).shift(
                1)
        self.sellCond = rep.sellCond.where(
            rep.sellCond != rep.sellCond.shift(1).fillna(rep.sellCond[0])
        ).shift(1)

        # might be a better solution cuz might not create copy - need to test it
        # taken from https://stackoverflow.com/questions/53608501/numpy-pandas-remove-sequential-duplicate-values-equivalent-of-bash-uniq-withou?noredirect=1&lq=1
        #         self.buyCond2 = rep.buyCond.where(rep.buyCond.ne(rep.buyCond.shift(1).fillna(rep.buyCond[0]))).shift(1)
        #         self.sellCond2 = rep.sellCond.where(rep.sellCond.ne(rep.sellCond.shift(1).fillna(rep.sellCond[0]))).shift(1)

        cond = [(self.buyCond == 1), (self.sellCond == 1)]
        out = ["Buy", "Sell"]
        self.all = np.select(cond, out, default=0)
        self.all = pd.DataFrame(
            self.all, index=rep.d.index, columns=[rep.name])
        self.all = self.all.replace("0", np.NAN)

        # find where first buy occured
        first_buy = self.buyCond.dropna().index[0]

        # drop all sell signals that come before first buy
        self.all = self.all[first_buy:]

        # remove all extra signals
        # https://stackoverflow.com/questions/19463985/pandas-drop-consecutive-duplicates
        # alternative, possibly faster solution ^
        # or using pd.ne()
        self.all = self.all[self.all != self.all.shift()]


class Agg_TradeSingal:
    """
    Aggregate version of TradeSignal that keeps trade signals for all stocks
    """

    def __init__(self):
        self.buys = pd.DataFrame()
        self.sells = pd.DataFrame()
        self.all = pd.DataFrame()


class TransPrice:
    """
    Find transaction price for current asset
    """

    def __init__(self, rep, ts, buyOn="Close", sellOn="Close"):
        # rep = rep
        self.all = ts.all
        self.buyCond = ts.buyCond
        self.sellCond = ts.sellCond

        buyIndex = self.all[self.all[rep.name] == "Buy"].index
        sellIndex = self.all[self.all[rep.name] == "Sell"].index

        self.buyPrice = rep.d[buyOn][buyIndex]
        self.sellPrice = rep.d[sellOn][sellIndex]

        self.buyPrice.name = rep.name
        self.sellPrice.name = rep.name

        cond = [(self.buyCond == 1), (self.sellCond == 1)]
        out = ["Buy", "Sell"]
        self.inTrade = np.select(cond, out, default=0)
        self.inTrade = pd.DataFrame(
            self.inTrade, index=rep.d.index, columns=[rep.name])
        self.inTrade = self.inTrade.replace("0", np.NAN)
        self.inTrade = self.inTrade.ffill().dropna()
        self.inTrade = self.inTrade[self.inTrade == "Buy"]

        self.buyPrice.name = "Entry"
        self.sellPrice.name = "Exit"

        df1 = self.buyPrice.reset_index()
        df2 = self.sellPrice.reset_index()

        self.trades = df1.join(
            df2, how="outer", lsuffix="_entry", rsuffix="_exit")

        # self.trades
        # replace hardcoded "Date_exit"
        self.trades["Date_exit"].fillna(rep.d.iloc[-1].name, inplace=True)
        self.trades[self.sellPrice.name].fillna(
            rep.d.iloc[1][sellOn], inplace=True)
        # alternative way
        #         u = self.trades.select_dtypes(exclude=['datetime'])
        #         self.trades[u.columns] = u.fillna(4)
        #         u = self.trades.select_dtypes(include=['datetime'])
        #         self.trades[u.columns] = u.fillna(5)

        self.trades["Symbol"] = rep.name

        self.inTradePrice = rep.d["Close"].loc[self.inTrade.index]
        self.inTradePrice.name = rep.name


# # old version
# class TransPrice_1(TradeSignal):
#     # inheriting from tradeSingal cuz of inTrade
#     """
#     Raw transaction price meaning only initial buy and sell prices are recorded without forward fill
#     """

#     def __init__(self, rep, buyOn="Close", sellOn="Close"):
#         # buy price & sell price
#         rep = rep
#         super().__init__(rep)
#         self.buyPrice = rep.d[buyOn][self.buyCond == 1]
#         self.sellPrice = rep.d[sellOn][self.sellCond == 1]

#         self.buyPrice.name = rep.name
#         self.sellPrice.name = rep.name

#         cond = [
#             (self.buyCond == 1),
#             (self.sellCond == 1)
#         ]
#         out = ["Buy", "Sell"]
#         self.inTrade = np.select(cond, out, default=0)
#         self.inTrade = pd.DataFrame(
#             self.inTrade, index=rep.d.index, columns=[rep.name])
#         self.inTrade = self.inTrade.replace("0", np.NAN)
#         self.inTrade = self.inTrade.ffill().dropna()
#         self.inTrade = self.inTrade[self.inTrade == "Buy"]

#         self.inTradePrice = rep.d["Close"].loc[self.inTrade.index]
#         self.inTradePrice.name = rep.name


class Agg_TransPrice:
    """
    Aggregate version of TransPrice that keeps transaction price for all stocks
    """

    def __init__(self):
        self.buyPrice = pd.DataFrame()
        self.sellPrice = pd.DataFrame()


#         self.inTrade = pd.DataFrame()
#         self.trades = pd.DataFrame()


class Trades:
    def __init__(self):
        self.trades = pd.DataFrame()
        #         self.inTrade = pd.DataFrame()
        self.weights = pd.DataFrame()
        self.inTradePrice = pd.DataFrame()


class Returns(TransPrice):
    """
    Calculates returns for the strategy
    """

    def __init__(self, rep):
        # rep = rep
        tp = TransPrice(rep)
        self.index = rep.d.index
        self.returns = pd.DataFrame(index=self.index, columns=[rep.name])
        # might result in errors tradesignal/execution is shifted
        self.returns[rep.name].loc[tp.buyPrice.index] = tp.buyPrice
        self.returns[rep.name].loc[tp.sellPrice.index] = tp.sellPrice
        self.returns = self.returns.dropna().pct_change()
        # works for now
        for i in self.returns.index:
            if tp.inTrade.loc[i][0] == "Buy":
                self.returns.loc[i] = -self.returns.loc[i]
        # self.returns.ffill(inplace=True)


class Stats:
    """
    Calculats various trade statistics based on returns
    """

    def __init__(self, rep):
        # rep = rep
        r = Returns(rep)
        self.posReturns = r.returns[r.returns > 0].dropna()
        self.negReturns = r.returns[r.returns < 0].dropna()
        self.posTrades = len(self.posReturns)
        self.negTrades = len(self.negReturns)
        self.meanReturns = r.returns.mean()
        self.hitRatio = self.posTrades / (self.posTrades + self.negTrades)
        self.totalTrades = self.posTrades + self.negTrades


class Portfolio:
    """
    Initial settings and what to calculate
    """

    def __init__(self):
        self.startAmount = 10000
        self.availAmount = self.startAmount
        self.value = pd.DataFrame()
        self.profit = pd.DataFrame()
        self.invested = pd.DataFrame()
        self.fees = pd.DataFrame()
        self.ror = pd.DataFrame()
        #         self.weights = pd.DataFrame()
        self.capUsed = pd.DataFrame()


port = Portfolio()
ats = Agg_TradeSingal()
atp = Agg_TransPrice()
t = Trades()

#############################################
# Generate signals part
#############################################


class Repeater:
    """
    Common class to avoid repetition
    """

    def __init__(self, d, buyCond, sellCond, name):
        self.d = d
        self.buyCond = buyCond
        self.sellCond = sellCond
        self.name = name


def prepricing():
    """
    Loop through files
    Generate signals
    Save them into common classes aggregate*
    """

    for name in data.data:
        current_asset = data.data[name]
        # separate strategy logic
        sma5 = SMA(current_asset, ["Close"], 5)
        sma25 = SMA(current_asset, ["Close"], 25)

        buyCond = sma5() > sma25()
        sellCond = sma5() < sma25()
        ################################

        rep = Repeater(current_asset, buyCond, sellCond, name)

        # find ts and tp for an asset
        ts = TradeSignal(rep)
        tp = TransPrice(rep, ts)
        #         ret = Returns(rep)

        # save ts and tp for portfolio level
        ats.buys = pd.concat([ats.buys, ts.buyCond], axis=1)
        ats.sells = pd.concat([ats.sells, ts.sellCond], axis=1)
        ats.all = pd.concat([ats.all, ts.all], axis=1)

        #         atp.inTrade = pd.concat([atp.inTrade, tp.inTradePrice], axis=1)
        atp.buyPrice = pd.concat([atp.buyPrice, tp.buyPrice], axis=1)
        atp.sellPrice = pd.concat([atp.sellPrice, tp.sellPrice], axis=1)
        #         atp.trades = pd.concat([atp.trades, tp.trades], axis=0)
        t.trades = pd.concat([t.trades, tp.trades], axis=0)
        t.inTradePrice = pd.concat([t.inTradePrice, tp.inTradePrice], axis=1)

        # for testing
        # ats.all["Date"].dt.tz_localize(None)
        ats.all.to_sql("ats_all", con, if_exists="replace")
        ats.buys.to_sql("ats_buys", con, if_exists="replace")
        ats.sells.to_sql("ats_sells", con, if_exists="replace")

        atp.buyPrice.to_sql("atp_buy_price", con, if_exists="replace")
        atp.sellPrice.to_sql("atp_sell_price", con, if_exists="replace")


#         t.inTradePrice = pd.concat([t.inTradePrice, tp.inTradePrice], axis=1)
#         port.tp = pd.concat([port.tp, tp.inTrade], axis=1)
#         port.ror = pd.concat([port.ror, ret.returns], axis=1)
#         port.inTrade = pd.concat([port.inTrade, tp.inTradePrice], axis=1)
#         port.transPrice = pd.concat([port.transPrice, tp.buyPrice], axis=1)
#         print(port.accRet)
# stats = Stats(rep)


#############################################
# Calculate portfolio part
#############################################
def roll_prev_value(df, current_bar, prev_bar):
    df.loc[current_bar] = df.iloc[prev_bar]


def run_portfolio():
    """
    Calculate profit and loss for the stretegy
    """
    # prepare data for portfolio
    prepricing()

    # prepare portfolio level
    # copy index and column names for weights
    t.weights = pd.DataFrame(
        index=t.inTradePrice.index, columns=t.inTradePrice.columns)
    # t.priceChange = t.inTradePrice - t.inTradePrice.shift()

    # Fill with 0s, otherwise results in NaN for port.availAmount
    # t.priceChange.fillna(0, inplace=True)

    # prepare value, avail amount, invested
    # copy index and column names for portfolio change
    port.value = pd.DataFrame(
        index=t.inTradePrice.index, columns=["Portfolio value"])
    port.value.iloc[0] = port.startAmount

    # copy index and set column name for avail amount
    port.availAmount = pd.DataFrame(
        index=t.inTradePrice.index, columns=["Available amount"])
    port.availAmount.iloc[0] = port.startAmount
    # port.availAmount.ffill(inplace=True)

    # copy index and column names for invested amount
    port.invested = pd.DataFrame(
        index=t.inTradePrice.index, columns=t.weights.columns)
    port.invested.iloc[0] = 0
    # put trades in chronological order
    # t.trades.sort_values("Date_entry", inplace=True)
    # t.trades.reset_index(drop=True, inplace=True)

    # set weights to 0 when exit
    # t.weights.loc[atp.sellPrice.index] = 0

    # change column names to avoid error

    atp.buyPrice.columns = t.weights.columns
    atp.sellPrice.columns = t.weights.columns

    # atp.buyPrice2 = pd.DataFrame(index=t.inTradePrice.index)
    # atp.buyPrice2 = pd.concat([atp.buyPrice2, atp.buyPrice], axis=1)
    # atp.buyPrice2.ffill(inplace=True)

    # run portfolio level
    # allocate weights
    for current_bar, row in port.availAmount.iterrows():
        # weight = port value / entry
        # return_prev_bar()
        prev_bar = port.availAmount.index.get_loc(current_bar) - 1

        # not -1 cuz it will replace last value
        if prev_bar != -1:
            # update avail amount (roll)
            # port.availAmount.loc[current_bar] = port.availAmount.iloc[prev_bar]
            roll_prev_value(port.availAmount, current_bar, prev_bar)

            # update invested amount (roll)
            # port.invested.loc[current_bar] = port.invested.iloc[prev_bar]
            roll_prev_value(port.invested, current_bar, prev_bar)

            # update weight anyway cuz if buy, the wont roll for other stocks (roll)
            # t.weights.loc[current_bar] = t.weights.iloc[prev_bar]
            roll_prev_value(t.weights, current_bar, prev_bar)

        # if there was an entry on that date
        # allocate weight
        # update avail amount (subtract)
        if current_bar in atp.buyPrice.index:
            # find amount to be invested
            to_invest = port.availAmount.loc[current_bar,
                                             "Available amount"] * 0.1
            # find assets that need allocation
            # those that dont have buyPrice for that day wil have NaN
            # drop them, keep those that have values
            affected_assets = atp.buyPrice.loc[current_bar].dropna(
            ).index.values

            # find current bar, affected assets
            # allocate shares to all assets = invested amount/buy price
            t.weights.loc[current_bar, affected_assets] = (
                to_invest / atp.buyPrice.loc[current_bar, affected_assets])

            # update portfolio invested amount
            port.invested.loc[current_bar, affected_assets] = to_invest

            # update portfolio avail amount -= sum of all invested money that day
            port.availAmount.loc[current_bar] -= port.invested.loc[
                current_bar].sum()

        # if there was an exit on that date
        # set weight to 0
        # update avail amount
        if current_bar in atp.sellPrice.index:
            # prob need to change this part for scaling implementation
            affected_assets = atp.sellPrice.loc[current_bar].dropna().index.values
            # amountRecovered = t.weights.loc[current_bar, affected_assets] * atp.buyPrice2.loc[current_bar, affected_assets]
            port.availAmount.loc[current_bar] += port.invested.loc[
                current_bar, affected_assets].sum()

            # set invested amount of the assets to 0
            port.invested.loc[current_bar, affected_assets] = 0

            # set weight to 0
            t.weights.loc[current_bar, affected_assets] = 0

    # testing
    df_all = pd.concat([port.availAmount, port.invested], axis=1)
    df_all.to_sql("df_all", con, if_exists="replace")
    t.weights.to_sql("t_weights", con, if_exists="replace")
    port.availAmount.to_sql("port_avail_amount", con, if_exists="replace")
    port.invested.to_sql("port_invested", con, if_exists="replace")

    # # if no new trades/exits
    # # update weight
    # else:
    #     t.weights.loc[current_bar] = t.weights.iloc[prev_bar]
    #     pass
    #         prev_bar = port.availAmount.index.get_loc(current_bar) - 1
    #         if prev_bar != -1:
    #             port.availAmount.loc[current_bar] = port.availAmount.iloc[prev_bar]
    # update avail amount for gains/losses that day
    # done in the end to avoid factroing it in before buy
    # if != -1 to skip first row
    # if prev_bar != -1:
    #     port.availAmount.loc[current_bar] += (
    #         t.priceChange.loc[current_bar] * t.weights.loc[current_bar]).sum()

    # profit = weight * chg
    # portfolio value += profit


run_portfolio()
