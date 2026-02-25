import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from Backtest import constants as C
from Backtest.settings import Settings
from Backtest.utils import _find_signals, _remove_dups

@dataclass
class TradeSignal:
    """
    Find trade signals for current asset
    For now, long only. 1 where buy, 0 where sell
    Possibly add signal shift - added for now to match excel results

    Inputs:
        - buy_cond (from repeater): raw, contains all signals
        - sell_cond (from repeater): raw, contains all signals
    Output:
        - buy_cond: buy results where signals switch from 1 to 0, 0 to 1, etc
        - sell_cond: sell results where signals switch from 1 to 0, 0 to 1, etc
        - all: all results with Buy and Sell
    """
    buy_cond: pd.DataFrame = field(default_factory=pd.DataFrame)
    sell_cond: pd.DataFrame = field(default_factory=pd.DataFrame)
    short_cond: pd.DataFrame = field(default_factory=pd.DataFrame)
    cover_cond: pd.DataFrame = field(default_factory=pd.DataFrame)
    all_: pd.DataFrame = field(default_factory=pd.DataFrame)
    _buy_shift: pd.DataFrame = field(default_factory=pd.DataFrame)
    _sell_shift: pd.DataFrame = field(default_factory=pd.DataFrame)
    _short_shift: pd.DataFrame = field(default_factory=pd.DataFrame)
    _cover_shift: pd.DataFrame = field(default_factory=pd.DataFrame)
    long: pd.DataFrame = field(default_factory=pd.DataFrame)
    short: pd.DataFrame = field(default_factory=pd.DataFrame)
    all_merged: pd.DataFrame = field(default_factory=pd.DataFrame)

    def run(self, rep):
        # ? Can probably be optimized by smashing everything onto 1 series -> ffill() -> remove dups
        # buy/sell/short/cover/all signals
        self.buy_cond = _find_signals(rep.all_cond[C.BUY])
        self.short_cond = _find_signals(rep.all_cond[C.SHORT])

        # keeping it here for now
        # TODO: move it somewhere else (postprocess/logic)
        # from Backtest.indicators import ATR
        # atr = ATR(rep.data, 14)

        # _apply_stop("buy", self.buy_cond, rep, atr()*2)
        # _apply_stop("short", self.short_cond, rep, atr()*2)

        self.sell_cond = _find_signals(rep.all_cond[C.SELL])
        self.cover_cond = _find_signals(rep.all_cond[C.COVER])

        # delay implementation
        self._buy_shift = self.buy_cond.shift(Settings.buy_delay)
        self._sell_shift = self.sell_cond.shift(Settings.sell_delay)
        self._short_shift = self.short_cond.shift(Settings.short_delay)
        self._cover_shift = self.cover_cond.shift(Settings.cover_delay)

        self.all_ = pd.concat([self._buy_shift, self._sell_shift, self._short_shift, 
                            self._cover_shift], axis=1)
        self.all_.index.name = "Date"
        # might be a better solution cuz might not create copy - need to test it
        # taken from https://stackoverflow.com/questions/53608501/numpy-pandas-remove-sequential-duplicate-values-equivalent-of-bash-uniq-withou?noredirect=1&lq=1
        # self.buy_cond2 = rep.buy_cond.where(rep.buy_cond.ne(rep.buy_cond.shift(1).fillna(rep.buy_cond[0]))).shift(1)
        # self.sell_cond2 = rep.sell_cond.where(rep.sell_cond.ne(rep.sell_cond.shift(1).fillna(rep.sell_cond[0]))).shift(1)
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

@dataclass
class AggTradeSignal:
    """
    Aggregate version of TradeSignal that keeps trade signals for all stocks
    """
    buys: pd.DataFrame = field(default_factory=pd.DataFrame)
    sells: pd.DataFrame = field(default_factory=pd.DataFrame)
    shorts: pd.DataFrame = field(default_factory=pd.DataFrame)
    covers: pd.DataFrame = field(default_factory=pd.DataFrame)
    all: pd.DataFrame = field(default_factory=pd.DataFrame)

@dataclass
class TransPrice:
    """
    Looks up transaction price for current asset

    Inputs:
        - time series (from repeater)
        - trade_singals: DataFrame containing all buys and sells
        - buyOn (optional): column that should be looked up when buy occurs
        - sellOn (optional): column that should be looked up when sell occurs

    Output:
        - buy_price: used in Trades to generate trade list
        - sell_price: used in Trades to generate trade list 
        - buy_index: dates of buy_price
        - sell_index: dates of sell_price
    """
    all_: pd.DataFrame = field(default_factory=pd.DataFrame)
    buy_index: pd.DataFrame = field(default_factory=pd.DataFrame)
    sell_index: pd.DataFrame = field(default_factory=pd.DataFrame)
    short_index: pd.DataFrame = field(default_factory=pd.DataFrame)
    cover_index: pd.DataFrame = field(default_factory=pd.DataFrame)
    buy_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    sell_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    short_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    cover_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    
    def run(self, rep, trade_signals):
        self.all_ = trade_signals.all_merged

        self.buy_index = self.all_[self.all_[C.LONG]==C.BUY].index
        self.sell_index = self.all_[self.all_[C.LONG]==C.SELL].index
        self.short_index = self.all_[self.all_[C.SHORT]==C.SHORT].index
        self.cover_index = self.all_[self.all_[C.SHORT]==C.COVER].index

        self.buy_price = rep.data[Settings.buy_on][self.buy_index]
        self.sell_price = rep.data[Settings.sell_on][self.sell_index]
        self.short_price = rep.data[Settings.short_on][self.short_index]
        self.cover_price = rep.data[Settings.cover_on][self.cover_index]

        self.buy_price.name = rep.name
        self.sell_price.name = rep.name
        self.short_price.name = rep.name
        self.cover_price.name = rep.name

@dataclass
class AggTransPrice:
    """
    Aggregate version of TransPrice that keeps transaction price for all stocks
    """
    buy_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    sell_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    short_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    cover_price: pd.DataFrame = field(default_factory=pd.DataFrame)

@dataclass
class Trades:
    """
    Generates DataFrame with trade entries, exits, and transaction prices

    Inputs:
        - trade_signals: Raw buys and sells to find trade duration
        - trans_prices: Transaction prices that need to be matched
    Outputs:
        - trades: DataFrame with trade entry, exits, and transaction prices
        - in_trade: DataFrame that shows time spent in trade for current asset
        - in_trade_price: Close price for the time while in_trade
    """
    trades: pd.DataFrame = field(default_factory=pd.DataFrame)
    in_trade: pd.DataFrame = field(default_factory=pd.DataFrame)
    in_trade_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    price_fluctuation_dollar: pd.DataFrame = field(default_factory=pd.DataFrame)
       
    def run(self, rep, trade_signals, trans_prices):
        self.trades = pd.DataFrame()
        self.in_trade = pd.DataFrame()
        self.in_trade_price = pd.DataFrame()

        self.in_trade = trade_signals.all_merged #long, short
        self.in_trade = self.in_trade.ffill()
        self.in_trade = self.in_trade[(self.in_trade[C.LONG] == C.BUY) | (self.in_trade[C.SHORT] == C.SHORT)]

        long = trans_prices.buy_price.reset_index()
        sell = trans_prices.sell_price.reset_index()
        short = trans_prices.short_price.reset_index()
        cover = trans_prices.cover_price.reset_index()

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
        self.trades[trans_prices.sell_price.name + "_exit"] = self.trades[trans_prices.sell_price.name + "_exit"].fillna(
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

        self.in_trade_price = rep.data[C.CLOSE].loc[self.in_trade.index]
        self.in_trade_price.name = rep.name

        # finding dollar price change
        # ? use in_trade_price - in_trade_price.shift(1) ?
        self.price_fluctuation_dollar = rep.data[C.CLOSE] - rep.data[C.CLOSE].shift()
        self.price_fluctuation_dollar.name = rep.name

@dataclass
class AggTrades:
    """
    Aggregate version of Trades. Contains trades, weights, in_trade_price, priceFluctuation in dollars
    """
    trades: pd.DataFrame = field(default_factory=pd.DataFrame)
    weights: pd.DataFrame = field(default_factory=pd.DataFrame)
    price_fluctuation_dollar: pd.DataFrame = field(default_factory=pd.DataFrame)
    in_trade_price_fluc: pd.DataFrame = field(default_factory=pd.DataFrame)

@dataclass
class Repeater:
    """
    Common class to avoid repetition
    """
    data: pd.DataFrame = field(default_factory=pd.DataFrame)
    name: str = ""
    all_cond: pd.DataFrame = field(default_factory=pd.DataFrame)

@dataclass
class Cond:
    buy: pd.DataFrame = field(default_factory=pd.DataFrame)
    sell: pd.DataFrame = field(default_factory=pd.DataFrame)
    short: pd.DataFrame = field(default_factory=pd.DataFrame)
    cover: pd.DataFrame = field(default_factory=pd.DataFrame)
    all_: pd.DataFrame = field(default_factory=pd.DataFrame)

    def apply_stop(self, buy_or_short, current_asset, stop_length, trail="false"):
        if buy_or_short == C.BUY:
            temp_ind = current_asset[C.CLOSE] - stop_length
            temp = temp_ind[self.buy == 1]
        elif buy_or_short == C.SHORT:
            temp_ind = current_asset[C.CLOSE] + stop_length
            temp = temp_ind[self.short == 1]

        stops = pd.DataFrame(index=current_asset.index)
        temp.name = "stop"
        stops = stops.join(temp)
        stops = stops.ffill()
        stops = stops["stop"]

        if buy_or_short == C.BUY:
            self.sell = (current_asset[C.LOW] < stops) | self.sell
        elif buy_or_short == C.SHORT:
            self.cover = (current_asset[C.HIGH] > stops) | self.cover

    def _combine(self):
        if not self.buy.empty:
            self.all_[self.buy.name] = self.buy
            self.all_[self.sell.name] = self.sell
        if not self.short.empty:
            self.all_[self.short.name] = self.short
            self.all_[self.cover.name] = self.cover

        for df in [self.buy, self.sell, self.short, self.cover]:
            if df.name not in self.all_.columns:
                self.all_[df.name] = False
