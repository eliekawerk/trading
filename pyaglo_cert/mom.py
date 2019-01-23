#
# Python Module with Class
# for Vectorized Backtesting
# of Momentum-based Strategies
#
# Python for Algorithmic Trading
# (c) Dr. Yves J. Hilpisch
# The Python Quants GmbH
#
import numpy as np
import pandas as pd


class MomVectorBacktester(object):
    ''' Class for the vectorized backtesting of
    Momentum-based trading strategies.

    Attributes
    ==========
    symbol: str
       TR RIC (financial instrument) to work with
    start: str
        start date for data selection
    end: str
        end date for data selection
    amount: int, float
        amount to be invested at the beginning
    tc: float
        proportional transaction costs (e.g. 0.5% = 0.005) per trade

    Methods
    =======
    get_data:
        retrieves and prepares the base data set
    run_strategy:
        runs the backtest for the momentum-based strategy
    plot_results:
        plots the performance of the strategy compared to the symbol
    '''

    def __init__(self, currency_df, instrument, start, end, amount, tc):
        self.currency_df = currency_df
        self.instrument = instrument
        self.start = start
        self.end = end
        self.amount = amount
        self.tc = tc
        self.results = None
        self.compute_return()

    def compute_return(self):
        ''' Retrieves and prepares the data.
        '''
        
        ret = pd.DataFrame()
        ret['return'] = np.log(self.currency_df[self.instrument]/self.currency_df[self.instrument].shift(1))
        ret['price'] = self.currency_df[self.instrument]
        ret = ret.loc[self.start:self.end]
        self.data = ret
        

    def run_strategy(self, momentum=1):
        ''' Backtests the trading strategy.
        '''
        self.momentum = momentum
        data = self.data.copy().dropna()
        data['position'] = np.sign(data['return'].rolling(momentum).mean())
        data['strategy'] = data['position'].shift(1) * data['return']
        # determine when a trade takes place
        data.dropna(inplace=True)
        trades = data['position'].diff().fillna(0) != 0
        # subtract transaction costs from return when trade takes place
        data['strategy'][trades] -= self.tc
        data['creturns'] = self.amount * data['return'].cumsum().apply(np.exp)
        data['cstrategy'] = self.amount * \
            data['strategy'].cumsum().apply(np.exp)
        self.results = data
        # absolute performance of the strategy
        aperf = self.results['cstrategy'].iloc[-1]
        # out-/underperformance of strategy
        operf = aperf - self.results['creturns'].iloc[-1]
        return round(aperf, 2), round(operf, 2)

    def plot_results(self, ax=None, figsize=(16,6)):
        ''' Plots the cumulative performance of the trading strategy
        compared to the symbol.
        '''
        if self.results is None:
            print('No results to plot yet. Run a strategy.')
        title = '%s | TC = %.4f' % (self.instrument, self.tc)
        self.results[['creturns', 'cstrategy']].plot(title=title, ax=ax, figsize=figsize)


# if __name__ == '__main__':
#     mombt = MomVectorBacktester('AAPL.O', '2010-1-1', '2018-06-29',
#                                 10000, 0.0)
#     print(mombt.run_strategy())
#     print(mombt.run_strategy(momentum=2))
#     mombt = MomVectorBacktester('AAPL.O', '2010-1-1', '2018-06-29',
#                                 10000, 0.001)
#     print(mombt.run_strategy(momentum=2))