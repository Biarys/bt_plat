from collections import namedtuple

import numpy as np
import pandas as pd
import logging

TradeDetail = namedtuple('TradeDetail', 'datetime action price quantity commission strategy_name')
PnLDetail = namedtuple('PnLDetail', 'entry_time exit_time direction entry_price exit_price quantity commission')

from algo.ContartMockup import ContactSetup


class GenerateStrategyPnLData:

    def __init__(self, symbol, strategy_name, strategy_trade_data):
        self.symbol = symbol
        self.name = strategy_name
        self.trade_data = strategy_trade_data
        self.symbol_multiplier = ContactSetup.get(self.symbol).multiplier
        self.symbol_tick_size = ContactSetup.get(self.symbol).minimum_tick

        self._is_stack_processed = False
        self.buy_stack = []
        self.sell_stack = []

    def split_to_buy_sell_stack(self):
        buy_stack = self.trade_data[self.trade_data['action'].str.upper() == 'BUY']
        sell_stack = self.trade_data[self.trade_data['action'].str.upper() == 'SELL']

        self.buy_stack = [TradeDetail(row.datetime, row.action, row.price, row.quantity, row.commission,
                                      row.strategy_name) for row in buy_stack.itertuples()][::-1]

        self.sell_stack = [TradeDetail(row.datetime, row.action, row.price, row.quantity, row.commission,
                                       row.strategy_name) for row in sell_stack.itertuples()][::-1]

    def run_open_position_calc(self):
        if self._is_stack_processed is False:
            raise BrokenPipeError('self.run_pnl_calc() was not run prior to calculating the open position!')

        if len(self.buy_stack) == 0 and len(self.sell_stack) == 0:
            return []
        else:
            return self.buy_stack if len(self.buy_stack) > 0 else self.sell_stack

    def run_pnl_calc(self):

        pnl_stack = []
        while len(self.buy_stack) > 0 and len(self.sell_stack) > 0:
            _buy_trade = self.buy_stack.pop()
            _sell_trade = self.sell_stack.pop()

            if _buy_trade.datetime <= _sell_trade.datetime:
                entry_direction = 'LONG'
                entry_time = _buy_trade.datetime
                entry_price = _buy_trade.price
                exit_time = _sell_trade.datetime
                exit_price = _sell_trade.price
            else:
                entry_direction = 'SHORT'
                entry_time = _sell_trade.datetime
                entry_price = _sell_trade.price
                exit_time = _buy_trade.datetime
                exit_price = _buy_trade.price

            if _buy_trade.quantity == _sell_trade.quantity:
                closed_quantity = _buy_trade.quantity
                total_commission = _buy_trade.commission + _sell_trade.commission
            else:
                closed_quantity = min(_sell_trade.quantity, _buy_trade.quantity)
                remaining_quantity = int(max(_buy_trade.quantity, _sell_trade.quantity) - closed_quantity)
                total_commission = min(_buy_trade.commission, _sell_trade.commission) * 2
                remaining_commission = round(_buy_trade.commission + _sell_trade.commission - total_commission, 2)
                remaining_trade = TradeDetail(
                    datetime=_buy_trade.datetime if _buy_trade.quantity > _sell_trade.quantity else _sell_trade.datetime,
                    action=_buy_trade.action if _buy_trade.quantity > _sell_trade.quantity else _sell_trade.datetime,
                    price=_buy_trade.price if _buy_trade.quantity > _sell_trade.quantity else _sell_trade.price,
                    strategy_name=self.name, quantity=remaining_quantity, commission=remaining_commission)

                if _buy_trade.quantity > _sell_trade.quantity:
                    self.buy_stack.append(remaining_trade)
                else:
                    self.sell_stack.append(remaining_trade)

            pnl_stack.append(PnLDetail(entry_time=entry_time, exit_time=exit_time, entry_price=entry_price,
                                       exit_price=exit_price, direction=entry_direction, quantity=closed_quantity,
                                       commission=total_commission))

        try:
            # TODO: Create test cases where there are trades, but the position is not closed so it triggers this case
            pnl_data = pd.DataFrame(pnl_stack)
            pnl_data['pnl_tick'] = (
                                           pnl_data.exit_price - pnl_data.entry_price) * pnl_data.quantity / self.symbol_tick_size
            pnl_data['pnl_tick'] = np.where(pnl_data.direction == 'SHORT', pnl_data.pnl_tick * -1, pnl_data.pnl_tick)
            pnl_data['time_to_live'] = pnl_data.exit_time - pnl_data.entry_time
            pnl_data['pnl_amount'] = pnl_data.pnl_tick * self.symbol_multiplier * self.symbol_tick_size
            pnl_data['pnl_with_commission'] = pnl_data.pnl_amount - pnl_data.commission
            pnl_data['strategy_name'] = str(self.name)

        except AttributeError: pass
            # logging.info('PNL Dataframe does not contain any closed position')

        self._is_stack_processed = True
        return pnl_data