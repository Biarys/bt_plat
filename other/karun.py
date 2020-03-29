# TODO: I think this should be cache on Redis and saved to the DB OrderStatus Table and ExecutionReport Table
# TODO: I am thinking of removing the redis code for the broker and rely on in-memory variables
import threading
import logging
import redis
import os

from ibapi import wrapper
from ibapi.client import EClient
from ibapi.utils import iswrapper
from ibapi.common import TickerId, OrderId
from ibapi.order import Order
from ibapi.execution import  Execution, ExecutionFilter
from ibapi.commission_report import CommissionReport
from ibapi.order_state import OrderState
from ibapi.contract import Contract
from ibapi.account_summary_tags import AccountSummaryTags

from OrderMarkup import MarketOrder, LimitOrder, BracketOrder
from algo.execution.ExecutionHandler import ExecutionHandler
from ContartMockup import ContactSetup

log = logging.getLogger(__name__)

class OrderDetails:
    def __init__(self, **kwargs):
        self.order = kwargs.get('order', '')
        self.contract = kwargs.get('contract', '')
        self.order_status = kwargs.get('order_status', '')

    def __repr__(self):
        return f'{self.order}, {self.contract}, {self.order_status}'

class ExecutionDetails:
    def __init__(self, **kwargs):
        self.contract = kwargs.get('contract', '')
        self.execution = kwargs.get('execution', '')
        self.commission = kwargs.get('commission', '')

    def __repr__(self):
        return f'{self.execution}, {self.commission}'


class _IBClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)


class _IBWrapper(wrapper.EWrapper):
    def __init__(self):
        wrapper.EWrapper.__init__(self)


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

def singleton(myClass):
    instances = {}
    def getInstance(*args, **kwargs):
        if myClass not in instances:
            instances[myClass] = myClass(*args, **kwargs)
        return  instances[myClass]
    return getInstance

@singleton
class Broker(_IBWrapper, _IBClient, ExecutionHandler):
    _instance_created = False

    def __init__(self, clientId=1, **kwargs):
        _IBWrapper.__init__(self)
        _IBClient.__init__(self, wrapper=self)

        self.clientId = clientId
        self.tradable_account = os.environ['TWS_ACCOUNT']
        self.started = False
        self.nextValidOrderId = None
        self.globalCancelOnly = False

        self.order_table = {}
        self.execution_table = {}
        self.position_table = {}

        self.req_account_summary_tags = [AccountSummaryTags.AvailableFunds,
                                         AccountSummaryTags.LookAheadAvailableFunds]

        # self.redis_client = redis.Redis(host='redis', port=6379)
        # self.redis_client.set('connected_to_IB', 'False')

        self.available_funds = None
        self.lookahead_available_funds = None

        self.connect(host='tws', port=4003, clientId=self.clientId)
        ib_thread = threading.Thread(target=self.run, name="Interactive Broker Client Thread", )
        ib_thread.start()


    @iswrapper
    def connectAck(self):
        if self.asynchronous:
            self.startApi()

    @iswrapper
    def startApi(self):
        super().startApi()
        log.info('startAPi connection has been received')

    @iswrapper
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        log.info("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId

        log.info('Starting the request sequence')
        self.redis_client.set('connected_to_IB', 'True')
        self._start()

    @iswrapper
    def managedAccounts(self, accountsList:str):
        super().managedAccounts(accountsList)


        if self.tradable_account not in accountsList.split(','):
            log.error(f'Trading account: {self.tradable_account} not in [{accountsList}] '
                      f'TWS connected with the wrong account exiting connection for safety...')
            self.disconnect()

    @iswrapper
    def accountSummary(self, reqId:int, account:str, tag:str, value:str, currency:str):
        if tag == AccountSummaryTags.AvailableFunds:
            self.available_funds = float(value)

        if tag == AccountSummaryTags.LookAheadAvailableFunds:
            self.lookahead_available_funds = float(value)

        self.redis_client.set(tag, value)

    @iswrapper
    def position(self, account: str, contract: Contract, position: float, avgCost: float):

        if account.upper() == self.tradable_account.upper():
            self.position_table[contract.symbol] = {'contract': contract, 'position': position, 'avg_cost': avgCost}


    def _start(self):
        if self.started:
            return

        self.started = True

        if self.globalCancelOnly:
            self.reqGlobalCancel()

        self.reqAllOpenOrders()
        self.reqCurrentTime()
        self.reqAccountSummary(reqId=9000, groupName='All', tags=','.join(self.req_account_summary_tags))
        self.reqExecutions(reqId=9001, execFilter=ExecutionFilter())
        self.reqPositions()

    def _next_order_id(self) -> int:
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    @iswrapper
    def execDetails(self, reqId:int, contract:Contract, execution:Execution):
        e = ExecutionDetails(id=execution.execId, contract=contract, execution=execution, commission=None)
        self.execution_table[execution.execId] = e

    @iswrapper
    def commissionReport(self, commissionReport:CommissionReport):
        e = self.execution_table.get(commissionReport.execId, ExecutionDetails())
        self.execution_table[commissionReport.execId] = ExecutionDetails(id=commissionReport.execId, execution=e, contract=e.contract, commission=commissionReport)

    @iswrapper
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        log.error(f"Error. Id: {reqId} Code: {errorCode} Msg: {errorString}")

        is_connected_status = {
            1100: 'False',
            1102: 'True',
        }

        self.redis_client.set('connected_to_IB', is_connected_status.get(errorCode, 'true'))

    @iswrapper
    def openOrder(self, orderId:OrderId, contract:Contract, order:Order, orderState:OrderState):
        self.order_table[order.permId] = (OrderDetails(contract=contract, order=order, order_status=orderState))

    @iswrapper
    def orderStatus(self, orderId: OrderId, status: str, filled: float, remaining: float, avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        ord = self.order_table[permId]

        o = ord.order
        o.orderId = orderId
        o.filledQuantity = filled
        o.remainingQuantity = remaining
        o.avgFillPrice = avgFillPrice
        o.permId = permId
        o.parentId = parentId
        o.clientId = clientId

        s = ord.order_status
        s.status = status

        self.order_table[permId] = OrderDetails(contract=ord.contract, order=o, order_status=s)

    @iswrapper
    def disconnect(self):
        super().disconnect()
        self.redis_client.delete('connected_to_IB', *self.req_account_summary_tags)

    def place_order(self, symbol, action, quantity, limit_p=None, stop_loss_p=None, profit_target_p=None, **kwargs):
        symbol = ContactSetup.get(symbol, None)
        order_id = self._next_order_id()


        if symbol is None:
            log.error('Symbol Lookup failed, check to see if symbol is defined in ContractMockup...')
            return

        if stop_loss_p is None and profit_target_p is None:
            if limit_p:
                order = LimitOrder(quantity=quantity, action=action, limit_price=limit_p)
            else:
                order = MarketOrder(quantity=quantity, action=action)
        else:
            order = BracketOrder(parentOrderId=order_id, action=action, quantity=quantity, limit_price=limit_p, take_profit_price=profit_target_p, stop_loss_price=stop_loss_p)

        for o in order:
            self.placeOrder(orderId=order_id, contract=symbol, order=o)
            order_id = self._next_order_id()

    def process_orderbook(self, **kwargs):
        return

    def modify_order(self, modified_order:Order):
        try:
            order_detail = self.order_table[modified_order.permId]
            c = order_detail.contract
            o = order_detail.order
            self.placeOrder(orderId=o.orderId, contract=c, order=modified_order)

        except KeyError:
            log.error(f'Could not located order details for permId:{perm_order_id} to modify order...')

    def cancel_order(self, perm_order_id):
        try:
            req_order = self.order_table[perm_order_id]
            req_order = req_order.order

            # Cancel all bracket orders and parent if child order is given
            if req_order.parentId is not None:
                order_id = req_order.parentId
            else:
                order_id = req_order.orderId

            self.cancelOrder(orderId=order_id)

        except KeyError:
            log.error(f'Could not located order details for permId:{perm_order_id} to cancel order...')