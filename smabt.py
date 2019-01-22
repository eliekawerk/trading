# Modification of code written by Dr. Yves J. Hilpisch
# for Python for Algorithmic Trading
#
# Modifications are for fetching data
#
#
# Python Module with Class
# for Vectorized Backtesting
# of SMA-based Strategies
#
# Python for Algorithmic Trading
# (c) Dr. Yves J. Hilpisch
# The Python Quants GmbH
#
import numpy as np
import pandas as pd
from scipy.optimize import brute
import xarray as xr


class SMAVectorBacktester(object):
    ''' Class for the vectorized backtesting of SMA-based trading strategies.

    Attributes
    ==========
    symbol: str
        Google Finance symbol with which to work with
    SMA1: int
        time window in days for shorter SMA
    SMA2: int
        time window in days for longer SMA
    start: str
        start date for data retrieval
    end: str
        end date for data retrieval

    Methods
    =======
    get_data:
        retrieves and prepares the base data set
    set_parameters:
        sets one or two new SMA parameters
    run_strategy:
        runs the backtest for the SMA-based strategy
    plot_results:
        plots the performance of the strategy compared to the symbol
    update_and_run:
        updates SMA parameters and returns the (negative) absolute performance
    optimize_parameters:
        implements a brute force optimizeation for the two SMA parameters
    '''

    def __init__(self, price_series,instrument, SMA1, SMA2, start, end):
        self.price_series = price_series
        self.instrument = instrument
        self.SMA1 = SMA1
        self.SMA2 = SMA2
        self.start = start
        self.end = end
        self.results = None
        self.compute_factors()
        self.good_params = []

    def compute_factors(self):
        ''' Retrieves and prepares the data.
        '''
        factors = self.price_series.copy()
        
        factors['return'] = np.log(factors[self.instrument]/factors[self.instrument].shift(1))
        factors['SMA1'] = factors[self.instrument].rolling(self.SMA1).mean()
        factors['SMA2'] = factors[self.instrument].rolling(self.SMA2).mean()
        self.data = factors

    def set_parameters(self, SMA1=None, SMA2=None):
        ''' Updates SMA parameters and resp. time series.
        '''
        if SMA1 is not None:
            self.SMA1 = SMA1
            self.data['SMA1'] = self.data[self.instrument].rolling(self.SMA1).mean()
            
        if SMA2 is not None:
            self.SMA2 = SMA2
            self.data['SMA2'] = self.data[self.instrument].rolling(self.SMA2).mean()

    def run_strategy(self):
        ''' Backtests the trading strategy.
        '''
        data = self.data.copy().dropna()
        data['position'] = np.where(data['SMA1'] > data['SMA2'], 1, -1)
        data['strategy'] = data['position'].shift(1) * data['return']
        data.dropna(inplace=True)
        data['creturns'] = data['return'].cumsum().apply(np.exp)
        data['cstrategy'] = data['strategy'].cumsum().apply(np.exp)
        self.results = data
        # absolute performance of the strategy
        abs_perf = data['cstrategy'].iloc[-1]
        # out-/underperformance of strategy
        out_perf = abs_perf - data['creturns'].iloc[-1]
        return round(abs_perf, 2), round(out_perf, 2)

    def plot_results(self):
        ''' Plots the cumulative performance of the trading strategy
        compared to the symbol.
        '''
        if self.results is None:
            print('No results to plot yet. Run a strategy.')
        title = '%s | SMA1 = %d, SMA2 = %d' % (self.instrument,
                                               self.SMA1, self.SMA2)
        self.results[['creturns', 'cstrategy']].plot(title=title,
                                                     figsize=(10, 6))

    def update_and_run(self, SMA):
        ''' Updates SMA parameters and returns negative absolute performance
        (for minimazation algorithm).

        Parameters
        ==========
        SMA: tuple
            SMA parameter tuple
        '''
        self.set_parameters(int(SMA[0]), int(SMA[1]))
        
        strat = self.run_strategy()
        if strat[1] >= 0.2:
            param_dict = {}
            param_dict['SMA1'] = SMA[0]
            param_dict['SMA2'] = SMA[1]
            param_dict['out_perf'] = strat[1]
            self.good_params.append(param_dict)
            
        
        return -strat[0]

    def optimize_parameters(self, SMA1_range, SMA2_range):
        ''' Finds global maximum given the SMA parameter ranges.

        Parameters
        ==========
        SMA1_range, SMA2_range: tuple
            tuples of the form (start, end, step size)
        '''
        opt = brute(self.update_and_run, (SMA1_range, SMA2_range), finish=None)
        return opt, -self.update_and_run(opt)


