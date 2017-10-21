


def bsm_call_value(S0, K, T, r, sigma):
    '''

    Valuation of European call option in BSM model.
    Analytical Formula

    Parameters
    ==========
    S0 : float
        initial stock/index level
    K : float
        strike price
    T : float
        maturity date (in year fractions)
    r : float
        constant risk-free short rate
    sigma : float
        volatility factor in diffusion term

    Returns
    =======
    value : float
        present value of European call option

    '''

    from math import log, sqrt, exp
    from scipy import stats

    S0 = float(S0)
    d1 = (log(S0/K) + (r + 0.5 * sigma ** 2) * T)/ (sigma * sqrt(T))
    d2 = (log(S0/K) + (r - 0.5 * sigma ** 2) * T)/ (sigma * sqrt(T))
    value = (S0 * stats.norm.cdf(d1, 0.0, 1.0) - K )* exp(-r * T) * stats.norm.cdf(d2, 0.0, 1.0)
    return value

def bsm_vega(S0, K, T, r, sigma):
    '''
    Vega of European option in BSM Model

    Parameters
    ==========
    S0 : float
        initial stock/index level
    K : float
        strike price
    T : float
        maturity date (in year fractions)
    r : float
        constant risk-free short rate
    sigma : float
        volatility factor in diffusion term

    Returns
    =======
    vega: float
        partial derivative of BSM formula with respect to sigma
    '''

    from math import sqrt, log
    from scipy import stats

    S0 = float(S0)
    d1 = (log(S0/K) + (r + 0.5 * sigma ** 2) * T)/ (sigma * sqrt(T))
    vega = S0 * stats.norm.cdf(d1, 0.0, 1.0) *  sqrt(T)
    return vega


def bsm_call_imp_vol(S0, K, T, r, C0, sigma_est, it=100):
    '''
    Implied Volatility of European Call Option in BSM Model

    Parameters
    ==========
    S0 : float
        initial stock/index level
    K : float
        strike price
    T : float
        maturity date (in year fractions)
    r : float
        constant risk-free short rate
    sigma_est: float
        estimate of implied volatility
    it : integer
        number of iteration

    Returns
    =======
    sigma_est: float
        numericalluy estimated implied volatility
    '''        

    for i in range(it):
        bcv = (bsm_call_value(S0, K, T, r,sigma_est) - C0)

        bv = bsm_vega(S0, K, T, r, sigma_est)

        sigma_est -= (bcv/bv)
    return sigma_est




