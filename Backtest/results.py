import numpy as np
import pandas as pd
import logging

from Backtest import constants as C

logger = logging.getLogger(__name__)


def build_trade_list(trades, idx, keys, weights, start_amount) -> pd.DataFrame:
    """
    Builds the final trade list DataFrame from raw trade data and portfolio weights.

    Parameters:
        trades:       raw trades DataFrame from Agg_Trades
        idx:          time index of the portfolio
        keys:         list of asset names (must match weights column order)
        weights:      2D numpy array of portfolio weights (time x assets)
        start_amount: initial portfolio value

    Returns:
        DataFrame with per-trade entry/exit metrics
    """
    logger.info("Generating trade list.")
    trade_list = trades.copy()
    trade_list[C.DATE_EXIT] = trade_list[C.DATE_EXIT].astype(str)
    trade_list = trade_list.sort_values(by=[C.DATE_EXIT, C.DATE_ENTRY, C.SYMBOL])
    trade_list.reset_index(drop=True, inplace=True)

    # ! a work around failing dates, when buy and sell occur on the same candle -> a null row appears for entry stats
    # trade_list.dropna(inplace=True)

    trade_list[C.WEIGHT] = np.nan
    # ! temp putting stop loss value here
    trade_list[C.STOP_LOSS] = np.nan

    for asset in trade_list[C.SYMBOL].unique():
        asset_mask = trade_list[C.SYMBOL] == asset
        dates = trade_list.loc[asset_mask, C.DATE_ENTRY]
        asset_idx = trade_list.loc[asset_mask].index
        dates_locs = np.searchsorted(idx, dates)
        asset_loc = keys.index(asset)
        # ? might have problems with scaling (probably will)
        trade_list.loc[asset_idx, C.WEIGHT] = weights[dates_locs, asset_loc]

    # display positive weight for short trades (instead of negative shares)
    trade_list[C.WEIGHT] = np.where(
        trade_list[C.DIRECTION] == C.LONG,
        trade_list[C.WEIGHT],
        -trade_list[C.WEIGHT],
    )

    # $ change
    trade_list[C.DOLLAR_CHANGE] = trade_list[C.EXIT_PRICE] - trade_list[C.ENTRY_PRICE]

    # % change
    trade_list[C.PCT_CHANGE] = (
        trade_list[C.EXIT_PRICE] - trade_list[C.ENTRY_PRICE]
    ) / trade_list[C.ENTRY_PRICE]

    # $ profit
    trade_list[C.DOLLAR_PROFIT] = trade_list[C.WEIGHT] * trade_list[C.DOLLAR_CHANGE]
    trade_list[C.DOLLAR_PROFIT] = np.where(
        trade_list[C.DIRECTION] == C.LONG,
        trade_list[C.DOLLAR_PROFIT],
        -trade_list[C.DOLLAR_PROFIT],
    )

    # % profit
    trade_list[C.PCT_PROFIT] = np.where(
        trade_list[C.DIRECTION] == C.LONG,
        trade_list[C.PCT_CHANGE],
        -trade_list[C.PCT_CHANGE],
    )

    cum_profit = trade_list[C.DOLLAR_PROFIT].cumsum()
    trade_list[C.CUM_PROFIT] = cum_profit
    trade_list[C.PORTFOLIO_VALUE] = cum_profit + start_amount

    # position value at entry
    trade_list[C.POSITION_VALUE] = trade_list[C.WEIGHT] * trade_list[C.ENTRY_PRICE]

    # number of bars held
    exit_dt = pd.to_datetime(trade_list[C.DATE_EXIT], errors="coerce")
    trade_list[C.TRADE_DURATION] = exit_dt - trade_list[C.DATE_ENTRY]
    trade_list[C.TRADE_DURATION] = trade_list[C.TRADE_DURATION].fillna("Open")

    return trade_list
