
# Python Wrapper Class for
# Interactive Brokers API
# based on IbPy
#
# Python for Algorithmic Trading
# (c) Dr. Yves J. Hilpisch
# The Python Quants GmbH
#
from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.ext.TickType import TickType as TickType
from ib.opt.dispatcher import Dispatcher
from ib.opt.receiver import Receiver
from ib.opt.sender import Sender

import ib.opt
from time import sleep
import datetime as dt
import pandas as pd

EXCLUDE = ['contractDetails', 'contractDetailsEnd', 'position', 'positionEnd',
           'error', 'accountSummary', 'openOrder', 'orderStatus',
           'execDetails', 'commissionReport', 'connectionClosed',
           'nextValidId', 'tickGeneric', 'tickString', 'tickSnapshotEnd',
           'tickPrice', 'tickSize', 'historicalData']

VALID_BAR_SIZES = ['1 sec', '5 secs', '10 secs', '15 secs', '30 secs',
                   '1 min', '2 mins', '3 mins', '5 mins', '10 mins',
                   '15 mins', '20 mins', '30 mins', '1 hour', '2 hours',
                   '3 hours', '4 hours', '8 hours', '1 day', '1 week',
                   '1 month']

WHAT_TO_SHOW = ['TRADES', 'MIDPOINT', 'ASK', 'BID', 'BID_ASK',
                'HISTORICAL_VOLATILITY', 'OPTION_IMPLIED_VOLATILITY',
                'REBATE_RATE', 'FREE_RATE']

STREAMING_DATA_TYPES = ['lastTimestamp', 'askPrice', 'askSize',
                        'bidPrice', 'bidSize', 'low', 'high', 'close',
                        'volume', 'lastPrice', 'lastSize', 'halted']


class tpqib(ib.opt.Connection):
    """ This is a wrapper class for the Interactive Brokers API
    based on the IbPy package.
    """
    def __init__(self, host='127.0.0.1', port=7497, clientId=0,
                 receiver=None, sender=None, dispatcher=None):
        dispatcher = Dispatcher() if dispatcher is None else dispatcher
        receiver = Receiver(dispatcher) if receiver is None else receiver
        sender = Sender(dispatcher) if sender is None else sender
        super(tpqib, self).__init__(
            host, port, clientId, receiver, sender, dispatcher)
        self.reported_orders = set()
        self.next_order_id = None
        self.next_data_request_id = 1
        self.ticker_data = dict()
        self.tick_callbacks = dict()
        self.print_once = set()
        self.hist_data_loading = dict()
        self.hist_data = dict()
        try:
            self.connect()
        except:
            raise IOError("Can not connect %s on port %s" % (host, port))
        sleep(1)
        self.register_handlers()
        self.reqIds(-1)

    def register_handlers(self):
        self.register(self.on_next_order_id, 'NextValidId')
        self.register(self.on_contract_details, 'ContractDetails')
        self.register(self.on_position_request, 'Position')
        self.register(self.on_account_summary, 'AccountSummary')
        self.register(self.on_open_order, 'OpenOrder')
        self.register(self.on_order_status, 'OrderStatus')
        self.register(self.on_commission_report, 'CommissionReport')
        self.register(self.on_connection_closed, 'ConnectionClosed')
        self.register(self.on_tick, 'TickSize')
        self.register(self.on_tick, 'TickPrice')
        self.register(self.on_tick, 'TickGeneric')
        self.register(self.on_tick, 'TickString')
        self.register(self.on_snapshot_end, 'TickSnapshotEnd')
        self.register(self.on_hist_data, 'HistoricalData')
        self.register(self.error_handler, 'Error')
        self.registerAll(self.reply_handler)

    def create_contract(self, symbol, sec_type, exch, prim_exch, curr):
        ''' Create a contract object defining what will
        be purchased, at which exchange and in which currency.

        symbol - the ticker symbol for the contract
        sec_type - the security type for the contract ('STK' is 'stock')
        exch - the exchange to carry out the contract on
        prim_exch - the primary exchange to carry out the contract on
        curr - the currency in which to purchase the contract
        '''
        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = sec_type
        contract.m_exchange = exch
        contract.m_primaryExch = prim_exch
        contract.m_currency = curr
        return contract

    def create_order(self, order_type, quantity, action):
        ''' Create an Order object (Market/Limit) to go long/short.

        order_type - 'MKT', 'LMT' for market or limit orders
        quantity - integer number of units to order
        action - 'BUY' or 'SELL'
        '''
        order = Order()
        order.m_orderType = order_type
        order.m_totalQuantity = quantity
        order.m_action = action
        return order

    def place_order(self, contract, order):
        ''' Place the order for a given contract via TWS
        '''
        if not isinstance(contract, Contract):
            raise ValueError('contract must be a Contract object')
        if not isinstance(order, Order):
            raise ValueError('order must be an Order object')

        self.placeOrder(self.next_order_id, contract, order)
        self.next_order_id += 1

    def req_positions(self):
        ''' Wrapper to request from TWS a list of positions
        '''
        self.reqPositions()

    def req_contract_details(self, contract):
        ''' Wrapper to request from TWS details of a contract
        '''
        if not isinstance(contract, Contract):
            raise ValueError('contract must be a Contract object')
        self.reqContractDetails(1, contract)

    # setter and getter methods

    def set_next_order_id(self, order_id):
        ''' Sets the next order id, either when creating a new connection
        or when sending an open order
        '''
        self.next_order_id = order_id

    def get_next_order_id(self):
        return self.next_order_id

    def get_market_data_once(self, contract):
        ''' Sends a one time request for market data for a contract
        '''
        self.print_once.add(self.next_data_request_id)
        self.reqMktData(self.next_data_request_id, contract, '', True)
        self.next_data_request_id += 1

    def request_market_data(self, contract, callback=None):
        ''' Sends a market data request for a contract to TWS,
        the response is streamed to the callback function until
        the request is canceled with self.cancel_market_data(request_id);
        if no callback is given, the results will be printed;
        returns the request_id
        '''
        self.tick_callbacks[self.next_data_request_id] = callback
        self.reqMktData(self.next_data_request_id, contract, '', False)
        old = self.next_data_request_id
        self.next_data_request_id += 1
        return old

    def cancel_market_data(self, request_id):
        ''' Cancels a running market data request '''
        self.cancelMktData(request_id)

    def request_historical_data(self, contract, end_date_time, duration,
                                bar_size, what_to_show, use_RTH=True,
                                format_date=1):
        ''' Sends a historical data request for contract to the TWS,
        the response is streamed to the callback function until the request
        is canceld with self.cancel_market_data(request_id);
        returns the request_id
        '''
        if bar_size not in VALID_BAR_SIZES:
            raise ValueError('bar_size must be one of %s' % VALID_BAR_SIZES)
        if type(use_RTH) != bool:
            raise TypeError('use_RTH must be of type bool')
        elif use_RTH is True:
            use_RTH = True
        else:
            use_RTH = False
        if type(end_date_time) != dt.datetime:
            raise TypeError('end_date_time must be of type datetime')
        else:
            end_date_time = end_date_time.strftime('%Y%m%d %X')
        if what_to_show not in WHAT_TO_SHOW:
            raise ValueError('what_to_show must be in %s' % WHAT_TO_SHOW)

        self.reqHistoricalData(self.next_data_request_id, contract,
                               endDateTime=end_date_time, durationStr=duration,
                               barSizeSetting=bar_size, whatToShow=what_to_show,
                               useRTH=False, formatDate=format_date)

        # count request id

        old = self.next_data_request_id
        self.next_data_request_id += 1

        # set loading flag for request id

        self.hist_data_loading[old] = True
        return old

    def get_historical_data(self, request_id, silent=False):
        ''' Returns the historical data for a given request id,
        if silent equal to True, the 'is still loading' message will be ommited
        '''

        if request_id not in self.hist_data_loading:
            # unknown request id
            raise ValueError('No data found for request id %s' % request_id)
        elif self.hist_data_loading[request_id] is True:
            # still loading
            if not silent:
                print("Historical data for request id %s still loading" %
                      request_id)
            return False
        else:
            return self.hist_data[request_id]

    def is_historical_data_loading(self, request_id):
        ''' Returns True if the request with id request_id is still loading
        and False else
        '''
        if request_id not in self.hist_data_loading:
            # unknown request id
            raise ValueError('No data found for request id %s' % request_id)
        elif self.hist_data_loading[request_id] is True:
            # still loading
            return True
        else:
            return False

    # callback functions for requests to TWS

    def on_contract_details(self, msg):
        ''' Callback for connections
        '''
        summary = msg.contractDetails.m_summary
        templ = 'Symbol: %s, Type: %s, Exchange: %s, Primary Exchange: %s,'
        templ += 'Currency: %s'
        out = templ % (summary.m_symbol, summary.m_secType, summary.m_exchange,
                       summary.m_primaryExch, summary.m_currency)
        print(out)

    def on_position_request(self, msg):
        ''' Callback for position requests
        '''
        con = msg.contract
        pos = msg.pos
        avgCost = msg.avgCost
        templ = 'Quantity: %s, Symbol: %s, Currency: %s, Average Cost: %s'
        out = templ % (pos, con.m_symbol, con.m_currency, avgCost)
        print(out)

    def on_account_summary(self, msg):
        ''' Callback for account summary
        '''
        print('%s: %s' % (msg.tag, msg.value))

    def on_open_order(self, msg):
        ''' Callback for open orders
        '''
        con = msg.contract
        order = msg.order
        if msg.orderId not in self.reported_orders:
            self.reported_orders.add(msg.orderId)
            print('Order: %s %s %s' %
                  (order.m_action, order.m_totalQuantity, con.m_symbol))

    def on_order_status(self, msg):
        ''' Callback for order status
        '''
        print('    Filled: %s, Status: %s' % (msg.filled, msg.status))

    def on_commission_report(self, msg):
        pass

    def on_connection_closed(self, msg):
        ''' Callback for closing connection
        '''
        print('Bye.')

    def on_next_order_id(self, msg):
        ''' Callback for next order id requests
        '''
        self.set_next_order_id(msg.orderId)

    def on_tick(self, msg):
        ''' Callback for market data requests
        '''
        tick_id = msg.tickerId
        if msg.typeName == 'tickSize':
            field_name = TickType.getField(msg.field)
            value = msg.size
        elif msg.typeName == 'tickPrice':
            field_name = TickType.getField(msg.field)
            value = msg.price
        elif msg.typeName in ['tickGeneric', 'tickString']:
            field_name = TickType.getField(msg.tickType)
            value = msg.value

        if tick_id in self.print_once:
            if tick_id not in self.ticker_data:
                ticker_data = dict()
            else:
                ticker_data = self.ticker_data[tick_id]

            ticker_data[field_name] = value
            self.ticker_data[tick_id] = ticker_data
        elif tick_id in self.tick_callbacks:
            self.tick_callbacks[tick_id](field_name, value)

    def on_snapshot_end(self, msg):
        tick_id = msg.reqId
        tick_data = self.ticker_data[tick_id]
        timestamp = str(dt.datetime.fromtimestamp(int(tick_data['lastTimestamp'])))
        tick_string = 'Last timestamp: %s, Ask price: %s, Ask size: %s,'
        tick_string += 'Bid price: %s, Bid size: %s, Low: %s, '
        tick_string += 'High: %s, Close: %s, Volume: %s, Last price: %s,'
        tick_string += 'Last size: %s, Halted: %s'
        tick_string = tick_string % (timestamp,
                        tick_data['askPrice'], tick_data['askSize'],
                        tick_data['bidPrice'], tick_data['bidSize'],
                        tick_data['low'], tick_data['high'], tick_data['close'],
                        tick_data['volume'], tick_data['lastPrice'],
                        tick_data['lastSize'], tick_data['halted'])
        print(tick_string)

    def on_hist_data(self, msg):
        data_id = msg.reqId
        # columns = msg.keys()
        new_data = dict()
        for item in msg.items():
            if item[0] != 'reqId':
                if item[0] == 'date':
                    date = item[1]
                    if date[:8] == 'finished':
                        self.hist_data[data_id] = self.hist_data[
                            data_id].set_index('date')
                        self.hist_data_loading[data_id] = False
                        return True
                    elif len(date) == 8:
                        value = dt.datetime(
                            int(date[:4]), int(date[4:6]), int(date[6:]))
                    else:
                        time = date[9:].split(":")
                        value = dt.datetime(int(date[:4]), int(date[4:6]), int(
                          date[6:8]), int(time[0]), int(time[1]), int(time[2]))
                else:
                    value = item[1]

                new_data[item[0]] = value
        if data_id not in self.hist_data:
            temp_data = pd.DataFrame([new_data])
        else:
            temp_data = self.hist_data[data_id].append(
                new_data, ignore_index=True)
        self.hist_data[data_id] = temp_data

    def error_handler(self, msg):
        ''' Callback for errors
        '''
        if hasattr(msg, 'id') and msg.id in self.hist_data_loading:
            self.hist_data_loading[int(msg.id)] = False
            self.hist_data[int(msg.id)] = pd.DataFrame()

        print('Server Error: %s' % msg.errorMsg)

    def reply_handler(self, msg):
        ''' Callback for all other messages
        '''
        if msg.typeName not in EXCLUDE:
            print('Server Response: %s, %s' % (msg.typeName, msg))