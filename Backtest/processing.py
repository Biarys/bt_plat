import pandas as pd
import numpy as np
from Backtest import constants as C
from Backtest.Settings import Settings
from Backtest.utils import _find_signals, _remove_dups

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
    def __init__(self, rep):
        # ? Can probably be optimized by smashing everything onto 1 series -> ffill() -> remove dups
        # buy/sell/short/cover/all signals
        self.buyCond = _find_signals(rep.allCond[C.BUY])
        self.shortCond = _find_signals(rep.allCond[C.SHORT])

        # keeping it here for now
        # TODO: move it somewhere else (postprocess/logic)
        # from Backtest.indicators import ATR
        # atr = ATR(rep.data, 14)

        # _apply_stop("buy", self.buyCond, rep, atr()*2)
        # _apply_stop("short", self.shortCond, rep, atr()*2)

        self.sellCond = _find_signals(rep.allCond[C.SELL])
        self.coverCond = _find_signals(rep.allCond[C.COVER])

        # delay implementation
        self._buy_shift = self.buyCond.shift(Settings.buy_delay)
        self._sell_shift = self.sellCond.shift(Settings.sell_delay)
        self._short_shift = self.shortCond.shift(Settings.short_delay)
        self._cover_shift = self.coverCond.shift(Settings.cover_delay)

        self.all = pd.concat([self._buy_shift, self._sell_shift, self._short_shift, 
                            self._cover_shift], axis=1)
        self.all.index.name = "Date"
        # might be a better solution cuz might not create copy - need to test it
        # taken from https://stackoverflow.com/questions/53608501/numpy-pandas-remove-sequential-duplicate-values-equivalent-of-bash-uniq-withou?noredirect=1&lq=1
        # self.buyCond2 = rep.buyCond.where(rep.buyCond.ne(rep.buyCond.shift(1).fillna(rep.buyCond[0]))).shift(1)
        # self.sellCond2 = rep.sellCond.where(rep.sellCond.ne(rep.sellCond.shift(1).fillna(rep.sellCond[0]))).shift(1)
        # ! In case of buy and sell signal occuring on the same candle, Sell/Cover signal is prefered over Buy/Short
        # ! might create signal problems in the future
        cond = [(self._sell_shift == 1), (self._buy_shift == 1)]
        out = [C.SELL, C.BUY]
        self.long = self._merge_signals(cond, out, rep, self._buy_shift, C.LONG)

        cond = [(self._cover_shift == 1), (self._short_shift == 1)]
        out = [C.COVER, C.SHORT]
        self.short = self._merge_signals(cond, out, rep, self._short_shift, C.SHORT)

        self.all_merged = pd.concat([self.long, self.short], axis=1)
        self.all_merged.index.name = "Date"
    
        # remove all extra signals
        # https://stackoverflow.com/questions/19463985/pandas-drop-consecutive-duplicates
        # alternative, possibly faster solution ^
        # or using pd.ne()
        # or self.all = self.all[self.all != self.all.shift()]
        # self.all = _remove_dups(self.all)
        
    @staticmethod
    def _merge_signals(cond, out, rep, entry, col_name):        
        temp = entry.dropna()
        if not temp.empty:
            df = np.select(cond, out, default="")
            df = pd.DataFrame(df, index=rep.data.index, columns=[col_name])
            df = df.replace("", np.nan)

            # find where first buy occured
            first_entry = temp.index[0]

            df = df[first_entry:]
            df = _remove_dups(df)
        # if there are no position entries, return empty dataframe
        else:
            df = pd.DataFrame([], index=rep.data.index, columns=[col_name])

        return df
            


class Agg_TradeSingal:
    """
    Aggregate version of TradeSignal that keeps trade signals for all stocks
    """
    def __init__(self):
        self.buys = pd.DataFrame()
        self.sells = pd.DataFrame()
        self.shorts = pd.DataFrame()
        self.covers = pd.DataFrame()
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
    def __init__(self, rep, trade_signals):
        self.all = trade_signals.all_merged

        self.buyIndex = self.all[self.all[C.LONG]==C.BUY].index
        self.sellIndex = self.all[self.all[C.LONG]==C.SELL].index
        self.shortIndex = self.all[self.all[C.SHORT]==C.SHORT].index
        self.coverIndex = self.all[self.all[C.SHORT]==C.COVER].index

        self.buyPrice = rep.data[Settings.buy_on][self.buyIndex]
        self.sellPrice = rep.data[Settings.sell_on][self.sellIndex]
        self.shortPrice = rep.data[Settings.short_on][self.shortIndex]
        self.coverPrice = rep.data[Settings.cover_on][self.coverIndex]

        self.buyPrice.name = rep.name
        self.sellPrice.name = rep.name
        self.shortPrice.name = rep.name
        self.coverPrice.name = rep.name


class Agg_TransPrice:
    """
    Aggregate version of TransPrice that keeps transaction price for all stocks
    """
    def __init__(self):
        self.buyPrice = pd.DataFrame()
        self.sellPrice = pd.DataFrame()
        self.shortPrice = pd.DataFrame()
        self.coverPrice = pd.DataFrame()

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

        self.inTrade = trade_signals.all_merged #long, short
        self.inTrade = self.inTrade.ffill()
        self.inTrade = self.inTrade[(self.inTrade[C.LONG] == C.BUY) | (self.inTrade[C.SHORT] == C.SHORT)]

        long = trans_prices.buyPrice.reset_index()
        sell = trans_prices.sellPrice.reset_index()
        short = trans_prices.shortPrice.reset_index()
        cover = trans_prices.coverPrice.reset_index()

        long[C.DIRECTION] = C.LONG
        short[C.DIRECTION] = C.SHORT

        long = long.join(sell, how="outer", lsuffix="_entry", rsuffix="_exit")
        short = short.join(cover, how="outer", lsuffix="_entry", rsuffix="_exit")

        self.trades = pd.concat([long, short])
        # NAs should only be last values that are still open
        # ! commenting out for now since I dont want to change series type from dates to object
        self.trades[C.DATE_EXIT] = self.trades[C.DATE_EXIT].astype(str)
        self.trades[C.DATE_EXIT] = self.trades[C.DATE_EXIT].fillna("Open")

        # hardcoded Close cuz if still in trade, needs latest quote
        self.trades[trans_prices.sellPrice.name + "_exit"] = self.trades[trans_prices.sellPrice.name + "_exit"].fillna(
            rep.data.iloc[-1][C.CLOSE])
 
        # alternative way
        # u = self.trades.select_dtypes(exclude=['datetime'])
        # self.trades[u.columns] = u.fillna(4)
        # u = self.trades.select_dtypes(include=['datetime'])
        # self.trades[u.columns] = u.fillna(5)

        self.trades[C.SYMBOL] = rep.name

        # changing column names so it's easier to concat in self.agg_trades
        self.trades.rename(
            columns={
                rep.name + "_entry": C.ENTRY_PRICE,
                rep.name + "_exit": C.EXIT_PRICE,
            },
            inplace=True)

        self.inTradePrice = rep.data[C.CLOSE].loc[self.inTrade.index]
        self.inTradePrice.name = rep.name

        # finding dollar price change
        # ? use inTradePrice - inTradePrice.shift(1) ?
        self.priceFluctuation_dollar = rep.data[C.CLOSE] - rep.data[C.CLOSE].shift()
        self.priceFluctuation_dollar.name = rep.name


class Agg_Trades:
    """
    Aggregate version of Trades. Contains trades, weights, inTradePrice, priceFluctuation in dollars
    """
    def __init__(self):
        self.trades = pd.DataFrame()
        self.weights = pd.DataFrame()
        self.priceFluctuation_dollar = pd.DataFrame()
        self.in_trade_price_fluc = pd.DataFrame()

class Repeater:
    """
    Common class to avoid repetition
    """
    def __init__(self, data, name, allCond):
        self.data = data
        self.name = name
        self.allCond = allCond

class Cond:
    def __init__(self):
        self.buy = pd.DataFrame()
        self.sell = pd.DataFrame()
        self.short = pd.DataFrame()
        self.cover = pd.DataFrame()
        self.all = pd.DataFrame()

    def _combine(self):
        if not self.buy.empty:
            self.all[self.buy.name] = self.buy
            self.all[self.sell.name] = self.sell
        if not self.short.empty:
            self.all[self.short.name] = self.short
            self.all[self.cover.name] = self.cover

        for df in [self.buy, self.sell, self.short, self.cover]:
            if df.name not in self.all.columns:
                self.all[df.name] = False
