import numpy as np
import pandas as pd
import logging
from Backtest.settings import Settings
from Backtest.utils import _find_affected_assets, _roll_prev_value_np

logger = logging.getLogger(__name__)

class Portfolio:
    """
    Initial settings and what to calculate
    """
    def __init__(self, price_fluctuation_dollar, idx, keys):
        self.weights = np.zeros((len(idx), len(keys)))
        self.value = np.array([0]*len(idx), dtype=float)
        self.value[0] = Settings.start_amount
        self.avail_amount = np.array([0]*len(idx), dtype=float)
        self.avail_amount[0] = Settings.start_amount
        self.profit = pd.DataFrame()
        self.invested = pd.DataFrame()
        self.fees = pd.DataFrame()
        self.ror = pd.DataFrame()
        self.capUsed = pd.DataFrame()
        self.equity_curve = pd.DataFrame()
        self.start_amount = Settings.start_amount
        self.price_fluctuation_dollar = price_fluctuation_dollar
        self.idx = idx
        self.keys = keys
        self.in_trade_price_fluc = np.zeros((len(self.idx), len(self.keys))) # float by default

    def run_portfolio(self, agg_trans_prices, agg_custom_stop):
        try:
            # allocate weights
            for current_bar in self.idx:
                # self.log.info(f"Processing {current_bar} - {self.idx.get_loc(current_bar)+1}/{len(self.idx)}")
                prev_bar = self.idx.get_loc(current_bar) - 1
                current_bar_int = prev_bar + 1

                # not -1 cuz it will replace last value
                if prev_bar != -1:
                    # update avail amount (roll)
                    _roll_prev_value_np(self.avail_amount, current_bar_int, prev_bar)

                    # update port value (roll)
                    _roll_prev_value_np(self.value, current_bar_int, prev_bar)

                    # update weight anyway cuz if buy, the wont roll for other stocks (roll)
                    _roll_prev_value_np(self.weights, current_bar_int, prev_bar)

                # if there was an entry on that date
                # allocate weight
                # update avail amount (subtract)
                self._execute_trades(current_bar, current_bar_int, agg_trans_prices, agg_custom_stop)

                # POST STEPS
                # record unrealized gains/losses
                self.in_trade_price_fluc[current_bar_int] = (self.price_fluctuation_dollar.iloc[
                    current_bar_int] * self.weights[current_bar_int]).values

                # update avail amount for bar's gain/loss            
                self._update_for_fluct_np(self.avail_amount, self.in_trade_price_fluc, current_bar, prev_bar, current_bar_int, agg_trans_prices)
                self._update_for_fluct_np(self.value, self.in_trade_price_fluc, current_bar, prev_bar, current_bar_int, agg_trans_prices)

            self._generate_equity_curve()
        except Exception as e:
            logger.exception(f"Error in run_portfolio: {e}")

    def _execute_trades(self, current_bar, current_bar_int, agg_trans_prices, agg_custom_stop):
        try:
            logger.debug(f"Executing trades for {current_bar}")
            if (current_bar in agg_trans_prices.buyPrice.index):
                self._execute_buy(current_bar, current_bar_int, agg_trans_prices, agg_custom_stop)

            if (current_bar in agg_trans_prices.sellPrice.index):
                self._execute_sell(current_bar, current_bar_int, agg_trans_prices)

            if (current_bar in agg_trans_prices.shortPrice.index):
                self._execute_short(current_bar, current_bar_int, agg_trans_prices, agg_custom_stop)

            if (current_bar in agg_trans_prices.coverPrice.index):
                self._execute_cover(current_bar, current_bar_int, agg_trans_prices)
        except Exception as e:
            logger.exception(f"Error in _execute_trades for {current_bar}: {e}")

    def _execute_buy(self, current_bar, current_bar_int, agg_trans_prices, agg_custom_stop):
        to_invest = self.value[current_bar_int]

        rounded_weights, affected_assets = self._position_sizer(to_invest, agg_trans_prices.buyPrice, current_bar, agg_custom_stop)

        self.weights[current_bar_int][affected_assets] = rounded_weights

        # find actualy amount invested
        actually_invested = self.weights[current_bar_int][affected_assets] * agg_trans_prices.buyPrice.loc[
                current_bar, affected_assets]

        self.avail_amount[current_bar_int] -= actually_invested.sum()

    def _execute_sell(self, current_bar, current_bar_int, agg_trans_prices):
        affected_assets = _find_affected_assets(agg_trans_prices.sellPrice, current_bar)
        self.avail_amount[current_bar_int] += (self.weights[current_bar_int][
            affected_assets] * agg_trans_prices.sellPrice.loc[
                current_bar, affected_assets]).sum()

        # set weight to 0
        self.weights[current_bar_int][affected_assets] = 0

    def _execute_short(self, current_bar, current_bar_int, agg_trans_prices, agg_custom_stop):
        to_invest = self.value[current_bar_int]
        
        rounded_weights, affected_assets = self._position_sizer(to_invest, agg_trans_prices.shortPrice, current_bar, agg_custom_stop)

        self.weights[current_bar_int][affected_assets] = -rounded_weights

        # find actualy amount invested
        actually_invested = self.weights[current_bar_int][affected_assets] * agg_trans_prices.shortPrice.loc[
                current_bar, affected_assets]

        self.avail_amount[current_bar_int] += actually_invested.sum()

    def _execute_cover(self, current_bar, current_bar_int, agg_trans_prices):
        affected_assets = _find_affected_assets(agg_trans_prices.coverPrice, current_bar)
        self.avail_amount[current_bar_int] += (self.weights[current_bar_int][
            affected_assets] * agg_trans_prices.coverPrice.loc[
                current_bar, affected_assets]).sum()

        # set weight to 0
        self.weights[current_bar_int][affected_assets] = 0

    def _position_sizer(self, port_value, trans_prices, current_bar, agg_custom_stop):
        if Settings.position_size_type == "pct":
            # $ amount
            to_invest = port_value * Settings.pct_invest

            affected_assets = _find_affected_assets(trans_prices, current_bar)

            # find current bar, affected assets
            # allocate shares to all assets = invested amount/buy price
            rounded_weights = to_invest / trans_prices.loc[
                current_bar, affected_assets]
            rounded_weights = rounded_weights.mul(
                10**Settings.round_to_decimals).apply(np.floor).div(
                    10**Settings.round_to_decimals)

            return rounded_weights, affected_assets

        elif Settings.position_size_type == "share":
            affected_assets = _find_affected_assets(trans_prices, current_bar)
            return Settings.position_size_value, affected_assets

        elif Settings.position_size_type == "amount":
            to_invest = Settings.position_size_value
            affected_assets = _find_affected_assets(trans_prices, current_bar)
            rounded_weights = to_invest / trans_prices.loc[
                current_bar, affected_assets]
            rounded_weights = rounded_weights.mul(
                10**Settings.round_to_decimals).apply(np.floor).div(
                    10**Settings.round_to_decimals)
            return rounded_weights, affected_assets

        elif Settings.position_size_type == "custom":
            affected_assets = _find_affected_assets(trans_prices, current_bar)
            to_invest = agg_custom_stop.loc[current_bar, affected_assets] * port_value
            rounded_weights = to_invest / trans_prices.loc[current_bar, affected_assets]
            rounded_weights = rounded_weights.mul(
                10**Settings.round_to_decimals).apply(np.floor).div(
                    10**Settings.round_to_decimals)
            return rounded_weights, affected_assets

    def _update_for_fluct_np(self, df, in_trade_adjust, current_bar, prev_bar, current_bar_int, agg_trans_prices):
        df[current_bar_int] += np.nansum(in_trade_adjust[current_bar_int])

        if current_bar in agg_trans_prices.buyPrice.index:
            if Settings.buy_on.capitalize()=="Close":            
                affected_assets = _find_affected_assets(agg_trans_prices.buyPrice, current_bar)
                df[current_bar_int] -= np.nansum(in_trade_adjust[current_bar_int, affected_assets])

        if current_bar in agg_trans_prices.shortPrice.index:
            if Settings.short_on.capitalize()=="Close":
                affected_assets = _find_affected_assets(agg_trans_prices.shortPrice, current_bar)
                df[current_bar_int] -= np.nansum(in_trade_adjust[current_bar_int][affected_assets])

        if current_bar in agg_trans_prices.sellPrice.index:
            if Settings.sell_on.capitalize()=="Close":
                affected_assets = _find_affected_assets(agg_trans_prices.sellPrice, current_bar)
                daily_adj = (self.weights[prev_bar][affected_assets] * 
                                self.price_fluctuation_dollar.iloc[current_bar_int][affected_assets]).sum()
                df[current_bar_int] += daily_adj

        if current_bar in agg_trans_prices.coverPrice.index:            
            if Settings.cover_on.capitalize()=="Close": 
                affected_assets = _find_affected_assets(agg_trans_prices.coverPrice, current_bar)
                daily_adj = (self.weights[prev_bar][affected_assets] * 
                                self.price_fluctuation_dollar.iloc[current_bar_int][affected_assets]).sum()
                df[current_bar_int] += daily_adj

    def _generate_equity_curve(self):
        try:
            logger.info("Generating equity curve")
            self.price_fluctuation_dollar.fillna(0, inplace=True)
            self.profit_daily_fluc_per_asset = self.weights * self.price_fluctuation_dollar
            self.equity_curve = self.profit_daily_fluc_per_asset.sum(1)
            self.equity_curve.iloc[0] = Settings.start_amount
            self.equity_curve = self.equity_curve.cumsum()
            self.equity_curve.name = "Equity"
        except Exception as e:
            logger.exception(f"Error in _generate_equity_curve: {e}")
