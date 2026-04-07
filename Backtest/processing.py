import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from Backtest import constants as C
from Backtest.settings import Settings
from Backtest.utils import _find_signals, _remove_dups

def _merge_signals(cond, out, rep, entry, col_name):
    temp = entry.dropna()
    if not temp.empty:
        df = np.select(cond, out, default="")
        df = pd.DataFrame(df, index=rep.data.index, columns=[col_name])
        df = df.replace("", np.nan)

        # find where first buy occured
        first_entry = temp.index[0]

        df = df.loc[first_entry:]
        df = _remove_dups(df)
    # if there are no position entries, return empty dataframe
    else:
        df = pd.DataFrame([], index=rep.data.index, columns=[col_name])

    return df

def generate_trade_signals(rep):
    """
    Find trade signals for current asset
    For now, long only. 1 where buy, 0 where sell
    Possibly add signal shift - added for now to match excel results

    Output:
        - all_merged: DataFrame containing all long and short merged positions
    """
    
    buy_cond = _find_signals(rep.all_cond[C.BUY])
    short_cond = _find_signals(rep.all_cond[C.SHORT])
    sell_cond = _find_signals(rep.all_cond[C.SELL])
    cover_cond = _find_signals(rep.all_cond[C.COVER])

    # delay implementation
    _buy_shift = buy_cond.shift(Settings.buy_delay)
    _sell_shift = sell_cond.shift(Settings.sell_delay)
    _short_shift = short_cond.shift(Settings.short_delay)
    _cover_shift = cover_cond.shift(Settings.cover_delay)

    cond_long = [(_sell_shift == 1), (_buy_shift == 1)]
    out_long = [C.SELL, C.BUY]
    long_df = _merge_signals(cond_long, out_long, rep, _buy_shift, C.LONG)

    cond_short = [(_cover_shift == 1), (_short_shift == 1)]
    out_short = [C.COVER, C.SHORT]
    short_df = _merge_signals(cond_short, out_short, rep, _short_shift, C.SHORT)

    all_merged = pd.concat([long_df, short_df], axis=1)
    all_merged.index.name = "Date"
    return all_merged

def match_trans_prices(rep, all_merged):
    """
    Looks up transaction price for current asset

    Inputs:
        - rep: Repeater holding asset data
        - all_merged: DataFrame containing all long and short trade signals

    Output:
        Returns a dictionary with 'buy_price', 'sell_price', 'short_price', 'cover_price'
    """
    buy_index = all_merged[all_merged[C.LONG]==C.BUY].index
    sell_index = all_merged[all_merged[C.LONG]==C.SELL].index
    short_index = all_merged[all_merged[C.SHORT]==C.SHORT].index
    cover_index = all_merged[all_merged[C.SHORT]==C.COVER].index

    buy_price = rep.data[Settings.buy_on][buy_index]
    sell_price = rep.data[Settings.sell_on][sell_index]
    short_price = rep.data[Settings.short_on][short_index]
    cover_price = rep.data[Settings.cover_on][cover_index]

    buy_price.name = rep.name
    sell_price.name = rep.name
    short_price.name = rep.name
    cover_price.name = rep.name
    
    return {
        "buy_price": buy_price,
        "sell_price": sell_price,
        "short_price": short_price,
        "cover_price": cover_price
    }

@dataclass
class AggTransPrice:
    """
    Aggregate version of TransPrice that keeps transaction price for all stocks
    """
    buy_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    sell_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    short_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    cover_price: pd.DataFrame = field(default_factory=pd.DataFrame)

def generate_trades(rep, all_merged, trans_prices_dict):
    """
    Generates DataFrame with trade entries, exits, and transaction prices

    Inputs:
        - rep: Repeater holding asset data
        - all_merged: raw trade signals from generate_trade_signals
        - trans_prices: dictionary of transaction prices
    Outputs:
        dictionary containing:
        - trades: DataFrame with trade entry, exits
        - price_fluctuation_dollar: dollar change
    """
    in_trade = all_merged.ffill()
    in_trade = in_trade[(in_trade[C.LONG] == C.BUY) | (in_trade[C.SHORT] == C.SHORT)]

    long = trans_prices_dict["buy_price"].reset_index()
    sell = trans_prices_dict["sell_price"].reset_index()
    short = trans_prices_dict["short_price"].reset_index()
    cover = trans_prices_dict["cover_price"].reset_index()

    long[C.DIRECTION] = C.LONG
    short[C.DIRECTION] = C.SHORT

    long = long.join(sell, how="outer", lsuffix="_entry", rsuffix="_exit")
    short = short.join(cover, how="outer", lsuffix="_entry", rsuffix="_exit")

    trades = pd.concat([long, short])
    trades[C.DATE_EXIT] = trades[C.DATE_EXIT].astype(str)
    trades[C.DATE_EXIT] = trades[C.DATE_EXIT].fillna("Open")

    # hardcoded Close cuz if still in trade, needs latest quote
    trades[rep.name + "_exit"] = trades[rep.name + "_exit"].fillna(rep.data.iloc[-1][C.CLOSE])

    trades[C.SYMBOL] = rep.name

    trades.rename(
        columns={
            rep.name + "_entry": C.ENTRY_PRICE,
            rep.name + "_exit": C.EXIT_PRICE,
        },
        inplace=True)

    # ? use in_trade_price - in_trade_price.shift(1) ?
    price_fluctuation_dollar = rep.data[C.CLOSE] - rep.data[C.CLOSE].shift()
    price_fluctuation_dollar.name = rep.name
    
    return {
        "trades": trades,
        "price_fluctuation_dollar": price_fluctuation_dollar
    }

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
class SingleAssetResult:
    """
    Holds the processed results for a single asset.
    """
    name: str = ""
    buy_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    sell_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    short_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    cover_price: pd.DataFrame = field(default_factory=pd.DataFrame)
    price_fluctuation_dollar: pd.DataFrame = field(default_factory=pd.DataFrame)
    trades: pd.DataFrame = field(default_factory=pd.DataFrame)
    custom_stop: pd.DataFrame = field(default_factory=pd.DataFrame)
    stop_length: pd.DataFrame = field(default_factory=pd.DataFrame)

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
