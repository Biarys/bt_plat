import pandas as pd
import numpy as np
import os
import abc

# own files
import data_reader
from indicators import *
import database_stuff as db
import config

# for testing
from datetime import datetime

#############################################
# Data reading
# Construct indicator
# Generate signals
# Find entries and exits
# Calculate portfolio value
# Generate portfolio statistics
#############################################

# TODO
# change how self.port.invested is implemented. Currently invests given ammount without round downs
# find common index for portflio. Should be a range of datetimes. Currently useing self.agg_trans_prices.priceFluctuation_dollar.index
# need to set/pass configs


#############################################
# Core starts
#############################################
class Backtest:

    def __init__(self, name="My Backtest"):
        self.name = name
        self.data = data_reader.DataReader()
        self.port = Portfolio()
        self.agg_trade_signals = Agg_TradeSingal()
        self.agg_trans_prices = Agg_TransPrice()
        self.agg_trades = Agg_Trades()
        self.trade_list = None

    def run(self):
        self.con, self.meta = db.connect(config.user, config.password,
                                         config.db)
        self.meta.reflect(bind=self.con)

        self.id = self.con.execute(
            "INSERT INTO \"backtests\" (name) VALUES ('{}') RETURNING backtest_id"
            .format(self.name)).fetchall()[0][0]  # fetchall() to get the tuple

        print(f"Backtest #{self.id} is running")

        self.data.readDB(self.con, self.meta, index_col="Date")
        # data.readCSVFiles(r"C:\Users\Biarys\Desktop\bt_plat\stock_data")

        self._run_portfolio(self.data)

    def _prepricing(self, data):
        """
        Loop through files
        Generate signals
        Find transaction prices
        Match buys and sells
        Save them into common classes agg_*
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

            # find trade_signals and trans_prices for an asset
            trade_signals = TradeSignal(rep)
            trans_prices = TransPrice(rep, trade_signals)
            trades_current_asset = Trades(rep, trade_signals, trans_prices)

            # save trade_signals for portfolio level
            self.agg_trade_signals.buys = pd.concat(
                [self.agg_trade_signals.buys, trade_signals.buyCond], axis=1)
            self.agg_trade_signals.sells = pd.concat(
                [self.agg_trade_signals.sells, trade_signals.sellCond], axis=1)
            self.agg_trade_signals.all = pd.concat(
                [self.agg_trade_signals.all, trade_signals.all], axis=1)

            # save trans_prices for portfolio level
            self.agg_trans_prices.buyPrice = pd.concat(
                [self.agg_trans_prices.buyPrice, trans_prices.buyPrice], axis=1)
            self.agg_trans_prices.sellPrice = pd.concat(
                [self.agg_trans_prices.sellPrice, trans_prices.sellPrice],
                axis=1)
            self.agg_trades.priceFluctuation_dollar = pd.concat([
                self.agg_trades.priceFluctuation_dollar,
                trades_current_asset.priceFluctuation_dollar
            ],
                                                                axis=1)
            self.agg_trades.trades = pd.concat(
                [self.agg_trades.trades, trades_current_asset.trades],
                axis=0,
                sort=True)
            self.agg_trades.inTradePrice = pd.concat([
                self.agg_trades.inTradePrice, trades_current_asset.inTradePrice
            ],
                                                     axis=1)

    def _run_portfolio(self, data):
        """
        Calculate profit and loss for the stretegy
        """

        # prepare data for portfolio
        self._prepricing(data)

        # prepare portfolio level
        # copy index and column names for weights
        self.port.weights = pd.DataFrame(
            index=self.agg_trades.priceFluctuation_dollar.index,
            columns=self.agg_trades.inTradePrice.columns)
        self.port.weights.iloc[0] = 0  # set starting weight to 0

        # nan in the beg cuz of .shift while finding priceFluctuation
        # to avoid nan in the beg
        self.agg_trades.priceFluctuation_dollar.iloc[0] = 0

        # Fill with 0s, otherwise results in NaN for self.port.avail_amount
        # self.agg_trades.priceChange.fillna(0, inplace=True)

        # prepare value, avail amount, invested
        # copy index and column names for portfolio change
        self.port.value = pd.DataFrame(
            index=self.agg_trades.priceFluctuation_dollar.index,
            columns=["Portfolio value"])
        self.port.value.iloc[0] = self.port.start_amount

        # copy index and set column name for avail amount
        self.port.avail_amount = pd.DataFrame(
            index=self.agg_trades.priceFluctuation_dollar.index,
            columns=["Available amount"])
        self.port.avail_amount.iloc[0] = self.port.start_amount
        # self.port.avail_amount.ffill(inplace=True)

        # copy index and column names for invested amount
        self.port.invested = pd.DataFrame(
            index=self.agg_trades.priceFluctuation_dollar.index,
            columns=self.port.weights.columns)
        self.port.invested.iloc[0] = 0
        # put trades in chronological order
        # trades_current_asset.trades.sort_values("Date_entry", inplace=True)
        # trades_current_asset.trades.reset_index(drop=True, inplace=True)

        # ! doesnt do anything
        # change column names to avoid error
        # self.agg_trans_prices.buyPrice.columns = self.port.weights.columns
        # self.agg_trans_prices.sellPrice.columns = self.port.weights.columns

        # run portfolio level
        # allocate weights
        for current_bar, row in self.port.avail_amount.iterrows():
            # weight = self.port value / entry

            prev_bar = self.port.avail_amount.index.get_loc(current_bar) - 1

            # not -1 cuz it will replace last value
            if prev_bar != -1:
                # update avail amount (roll)
                _roll_prev_value(self.port.avail_amount, current_bar, prev_bar)

                # update invested amount (roll)
                _roll_prev_value(self.port.invested, current_bar, prev_bar)

                # update weight anyway cuz if buy, the wont roll for other stocks (roll)
                _roll_prev_value(self.port.weights, current_bar, prev_bar)

            # if there was an entry on that date
            # allocate weight
            # update avail amount (subtract)
            if current_bar in self.agg_trans_prices.buyPrice.index:
                # find amount to be invested
                to_invest = self.port.avail_amount.loc[current_bar,
                                                       "Available amount"] * 0.1
                # find assets that need allocation
                # those that dont have buyPrice for that day wil have NaN
                # drop them, keep those that have values
                affected_assets = self.agg_trans_prices.buyPrice.loc[
                    current_bar].dropna().index.values

                # find current bar, affected assets
                # allocate shares to all assets = invested amount/buy price
                self.port.weights.loc[current_bar, affected_assets] = (
                    to_invest / self.agg_trans_prices.buyPrice
                    .loc[current_bar, affected_assets])

                # update portfolio invested amount
                self.port.invested.loc[current_bar, affected_assets] = to_invest

                # update portfolio avail amount -= sum of all invested money that day
                self.port.avail_amount.loc[
                    current_bar] -= self.port.invested.loc[
                        current_bar, affected_assets].sum()

            # if there was an exit on that date
            # set weight to 0
            # update avail amount
            if current_bar in self.agg_trans_prices.sellPrice.index:
                # prob need to change this part for scaling implementation

                # find assets that need allocation
                # those that dont have buyPrice for that day wil have NaN
                # drop them, keep those that have values
                affected_assets = self.agg_trans_prices.sellPrice.loc[
                    current_bar].dropna().index.values
                # amountRecovered = self.port.weights.loc[current_bar, affected_assets] * self.agg_trans_prices.buyPrice2.loc[current_bar, affected_assets]
                self.port.avail_amount.loc[
                    current_bar] += self.port.invested.loc[
                        current_bar, affected_assets].sum()

                # set invested amount of the assets to 0
                self.port.invested.loc[current_bar, affected_assets] = 0

                # set weight to 0
                self.port.weights.loc[current_bar, affected_assets] = 0

        # self.agg_trans_prices.priceFluctuation_dollar.fillna(0, inplace=True)
        # # find daily fluc per asset
        # self.port.profit_daily_fluc_per_asset = self.port.weights * self.agg_trans_prices.priceFluctuation_dollar
        # # find daily fluc for that day for all assets (sum of fluc for that day)
        # self.port.equity_curve = self.port.profit_daily_fluc_per_asset.sum(1)
        # # set starting amount
        # self.port.equity_curve.iloc[0] = self.port.start_amount
        # # apply fluctuation to equity curve
        # self.port.equity_curve = self.port.equity_curve.cumsum()
        # # self.port.equity_curve.columns
        # self.port.equity_curve.name = "Equity"
        _generate_equity_curve(self)

        # testing
        # self.port.value = self.port.equity_curve.sum()
        # ! doesnt do anything
        # self.port.weights.columns = ["w_" + col for col in self.port.weights.columns]

        # self.port.equity_curve.to_sql("equity_curve", self.con, if_exists="replace")
        # df_all = pd.concat([self.port.avail_amount, self.port.equity_curve], axis=1)
        # df_all.to_sql("df_all", self.con, if_exists="replace")
        # self.port.weights.to_sql("t_weights", self.con, if_exists="replace")
        # self.port.avail_amount.to_sql(
        #     "port_avail_amount", self.con, if_exists="replace")
        # self.port.invested.to_sql("port_invested", self.con, if_exists="replace")

        # profit = weight * chg
        # portfolio value += profit

        self._generate_trade_list()

    def _generate_trade_list(self):
        self.trade_list = self.agg_trades.trades
        self.trade_list.sort_values(by="Date_entry", inplace=True)
        self.trade_list.reset_index(drop=True, inplace=True)

        # $ change
        self.trade_list["Dollar_change"] = self.trade_list[
            "Exit_price"] - self.trade_list["Entry_price"]

        # % change
        self.trade_list["Pct_change"] = (
            self.trade_list["Exit_price"] -
            self.trade_list["Entry_price"]) / self.trade_list["Entry_price"]

        print(self.trade_list)


class TradeSignal:
    """
    Find trade signals for current asset
    For now, long only. 1 where buy, 0 where sell
    Possibly add signal shift - added for now to match excel results

    Inputs:
        - buyCond (from repeater): raw, contains all signals
        - sellCond (from repeater): raw, contains all signals
    Output:
        - buyCond: buy results where signals switch from 1 to 0, 0 to 1, etc
        - sellCond: sell results where signals switch from 1 to 0, 0 to 1, etc
        - all: all results with Buy and Sell
    """

    # not using self because the need to pass buy and sell cond
    # buyCond = buyCond.where(buyCond != buyCond.shift(1).fillna(buyCond[0])).shift(1)
    # sellCond = sellCond.where(sellCond != sellCond.shift(1).fillna(sellCond[0])).shift(1)

    def __init__(self, rep):
        # rep = rep
        # buy/sell/all signals
        self.buyCond = rep.buyCond.where(
            rep.buyCond != rep.buyCond.shift(1).fillna(rep.buyCond[0])).shift(1)
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
            self.all, index=rep.data.index, columns=[rep.name])
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
    Looks up transaction price for current asset

    Inputs:
        - time series (from repeater)
        - trade_singals: DataFrame containing all buys and sells
        - buyOn (optional): column that should be looked up when buy occurs
        - sellOn (optional): column that should be looked up when sell occurs
        
    Output:
        - buyPrice: used in Trades to generate trade list
        - sellPrice: used in Trades to generate trade list 
        - buyIndex: dates of buyPrice
        - sellIndex: dates of sellPrice
    """

    def __init__(self, rep, trade_signals, buyOn="Close", sellOn="Close"):
        self.all = trade_signals.all

        self.all = self.all.dropna()
        # to get rid of duplicates
        self.all = _remove_dups(self.all)
        # self.all = self.all.where(self.all != self.all.shift(1))

        self.buyIndex = self.all[self.all[rep.name] == "Buy"].index
        self.sellIndex = self.all[self.all[rep.name] == "Sell"].index

        self.buyPrice = rep.data[buyOn][self.buyIndex]
        self.sellPrice = rep.data[sellOn][self.sellIndex]

        self.buyPrice.name = rep.name
        self.sellPrice.name = rep.name


class Agg_TransPrice:
    """
    Aggregate version of TransPrice that keeps transaction price for all stocks
    """

    def __init__(self):
        self.buyPrice = pd.DataFrame()
        self.sellPrice = pd.DataFrame()


class Trades:
    """
    Generates DataFrame with trade entries, exits, and transaction prices

    Inputs:
        - trade_signals: Raw buys and sells to find trade duration
        - trans_prices: Transaction prices that need to be matched
    Outputs:
        - trades: DataFrame with trade entry, exits, and transaction prices
        - inTrade: DataFrame that shows time spent in trade for current asset
        - inTradePrice: Close price for the time while inTrade
    """

    def __init__(self, rep, trade_signals, trans_prices):
        self.trades = pd.DataFrame()
        self.inTrade = pd.DataFrame()
        self.inTradePrice = pd.DataFrame()

        self.inTrade = trade_signals.all
        self.inTrade = self.inTrade.ffill()
        self.inTrade = self.inTrade[self.inTrade[rep.name] == "Buy"]

        df1 = trans_prices.buyPrice.reset_index()
        df2 = trans_prices.sellPrice.reset_index()

        self.trades = df1.join(
            df2, how="outer", lsuffix="_entry", rsuffix="_exit")

        # replace hardcoded "Date_exit"
        # NAs should only be last values that are still open
        self.trades["Date_exit"].fillna(rep.data.iloc[-1].name, inplace=True)
        # hardcoded Close cuz if still in trade, needs latest quote
        self.trades[trans_prices.sellPrice.name + "_exit"].fillna(
            rep.data.iloc[-1]["Close"], inplace=True)
        # alternative way
        #         u = self.trades.select_dtypes(exclude=['datetime'])
        #         self.trades[u.columns] = u.fillna(4)
        #         u = self.trades.select_dtypes(include=['datetime'])
        #         self.trades[u.columns] = u.fillna(5)

        self.trades["Symbol"] = rep.name

        # changing column names so it's easier to concat in self.agg_trades
        self.trades.columns = [
            "Date_entry", "Entry_price", "Date_exit", "Exit_price", "Symbol"
        ]

        self.inTradePrice = rep.data["Close"].loc[self.inTrade.index]
        self.inTradePrice.name = rep.name

        # finding dollar price change
        self.priceFluctuation_dollar = rep.data["Close"] - rep.data[
            "Close"].shift()
        self.priceFluctuation_dollar.name = rep.name


class Agg_Trades:
    """
    Aggregate version of Trades. Contains trades, weights, inTradePrice, priceFluctuation in dollars
    """

    def __init__(self):
        self.trades = pd.DataFrame()
        # self.inTrade = pd.DataFrame()
        self.weights = pd.DataFrame()
        self.inTradePrice = pd.DataFrame()
        self.priceFluctuation_dollar = pd.DataFrame()


# ! not used
class Returns(TransPrice):
    """
    Calculates returns for the strategy
    """

    def __init__(self, rep):
        trans_prices = TransPrice(rep)
        self.index = rep.data.index
        self.returns = pd.DataFrame(index=self.index, columns=[rep.name])
        # might result in errors tradesignal/execution is shifted
        self.returns[rep.name].loc[
            trans_prices.buyPrice.index] = trans_prices.buyPrice
        self.returns[rep.name].loc[
            trans_prices.sellPrice.index] = trans_prices.sellPrice
        self.returns = self.returns.dropna().pct_change()
        # works for now
        for i in self.returns.index:
            if trans_prices.inTrade.loc[i][0] == "Buy":
                self.returns.loc[i] = -self.returns.loc[i]


# ! not used
class Stats:
    """
    Calculate various trade statistics based on returns
    """

    def __init__(self, rep):
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
        self.weights = pd.DataFrame()

        self.start_amount = 10000
        self.avail_amount = self.start_amount
        self.value = pd.DataFrame()
        self.profit = pd.DataFrame()
        self.invested = pd.DataFrame()
        self.fees = pd.DataFrame()
        self.ror = pd.DataFrame()
        self.capUsed = pd.DataFrame()
        self.equity_curve = pd.DataFrame()


class Repeater:
    """
    Common class to avoid repetition
    """

    def __init__(self, data, buyCond, sellCond, name):
        self.data = data
        self.buyCond = buyCond
        self.sellCond = sellCond
        self.name = name


# ? ##################
# ? Helper functions #
# ? ##################
def _roll_prev_value(df, current_bar, prev_bar):
    # might wanna return, just to be sure?
    df.loc[current_bar] = df.iloc[prev_bar]


def _generate_equity_curve(self):
    # Fillna cuz
    self.agg_trades.priceFluctuation_dollar.fillna(0, inplace=True)

    # find daily fluc per asset
    self.port.profit_daily_fluc_per_asset = self.port.weights * \
        self.agg_trades.priceFluctuation_dollar

    # find daily fluc for that day for all assets (sum of fluc for that day)
    self.port.equity_curve = self.port.profit_daily_fluc_per_asset.sum(1)

    # set starting amount
    self.port.equity_curve.iloc[0] = self.port.start_amount

    # apply fluctuation to equity curve
    self.port.equity_curve = self.port.equity_curve.cumsum()
    self.port.equity_curve.name = "Equity"


def _remove_dups(data):
    data = data.where(data != data.shift(1))
    return data


if __name__ == "__main__":
    b = Backtest("Strategy 1")
    # b.read_from_db
    # strategy logic
    b.run()
    print(b.agg_trades.trades)
    # b.show_results
