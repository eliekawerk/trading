#
# Python Script
# with SMA Trading Class
# for Interactive Brokers
#
# Python for Algorithmic Trading
# (c) Dr. Yves J. Hilpisch
# The Python Quants GmbH
#
import time
import tpqib
import pandas as pd
import datetime as dt


class ibSMATrader(object):
    def __init__(self, symbol, shares):
        ''' Initializes the trading class. '''
        self.con = tpqib.tpqib()
        self.symbol = symbol
        self.shares = shares
        self.contract = self.con.create_contract(symbol, 'STK', 'SMART',
                                                 'SMART', 'USD')
        self.details = self.con.req_contract_details(self.contract)
        self.buy_order = self.con.create_order('MKT', self.shares, 'Buy')
        self.sell_order = self.con.create_order('MKT', self.shares, 'Sell')
        self.data = pd.DataFrame()
        self.ticks = 0
        self.position = 0

    def define_strategy(self, field, value):
        ''' Defines the trading strategy logic. '''
        if field == 'bidPrice':
            self.ticks += 1
            timestamp = dt.datetime.now()
            self.data = self.data.append(pd.DataFrame({'bid': value},
                                                      index=[timestamp]))
            print('%3d ticks retrieved | ' %
                  self.ticks, timestamp, '| bid is %s' % value)

        elif field == 'askPrice':
            self.ticks += 1
            timestamp = dt.datetime.now()
            self.data = self.data.append(pd.DataFrame({'ask': value},
                                                      index=[timestamp]))
            print('%3d ticks retrieved | ' %
                  self.ticks, timestamp, '| ask is %s' % value)

        if field in ['askBid', 'askPrice']:
            self.resam = self.data.resample(
                '5s', label='right').last().ffill().iloc[:-1]

            if len(self.resam) >= 10:
                self.resam['SMA1'] = self.resam.mean(axis=1).rolling(5).mean()
                self.resam['SMA2'] = self.resam.mean(axis=1).rolling(10).mean()

                if (self.resam['SMA1'].iloc[-1] > self.resam['SMA2'].iloc[-1]) \
                        and (self.position == 0):
                    print('Creating buy order')
                    self.con.place_order(self.contract, self.buy_order)
                    self.position = 1

                elif (self.resam['SMA1'].iloc[-1] < self.resam['SMA2'].iloc[-1]) \
                        and (self.position == 1):
                    print('Creating sell order')
                    self.con.place_order(self.contract, self.sell_order)
                    self.position = 0

        if self.ticks == 50:
            if self.position == 1:
                self.con.place_order(self.contract, self.sell_order)
            self.con.cancel_market_data(self.request_id)
            self.con.close()

    def run_strategy(self):
        ''' Starts the automated execution. '''
        print('Starting automated trading strategy.')
        self.request_id = self.con.request_market_data(
            self.contract, self.define_strategy)


if __name__ == '__main__':
    sma = ibSMATrader('AAPL', 100)
    time.sleep(5)
    sma.run_strategy()
    while sma.con.isConnected():
        pass