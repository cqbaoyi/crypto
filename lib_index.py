######################################
##
##  Library of cryptocurrency index (with Websocket)
##  1) Indexation
##  2) CRT
##
##  Yi Bao  07/02/2018
##
######################################
from pymongo import *
import pandas as pd
import numpy as np
import bisect
import time

''' compute crypto index using snapshot of asks and bids'''
def cryptoindex(asks, bids, C = 100, D = 0.005):
    if not asks or not bids:
        print(asks)
        raise Exception("Exception: empty Order book")
    a_p = [x[0] for x in asks]
    b_p = [x[0] for x in bids]
    ''' capped order size '''
    a_v = [x[1] if x[1] <= C else C for x in asks]
    b_v = [x[1] if x[1] <= C else C for x in bids]
    ''' cumu vol '''
    cumu_a_v = np.cumsum(a_v)
    cumu_b_v = np.cumsum(b_v)
    askPV = [a_p[bisect.bisect_left(cumu_a_v, v)] if v <= cumu_a_v[-1] else a_p[-1] for v in range(1, C + 1)]
    bidPV = [b_p[bisect.bisect_left(cumu_b_v, v)] if v <= cumu_b_v[-1] else b_p[-1] for v in range(1, C + 1)]
    midPV = [(askPV[i] + bidPV[i]) / 2.0 for i in range(len(askPV))]
    midSV = [askPV[i] / bidPV[i] - 1.0 for i in range(len(askPV))]
    ''' utilized depth '''
    uti_depth = (C if D > midSV[-1] else bisect.bisect_left(midSV, D))
    uti_depth = max(uti_depth, 1.0)
    lam = 1.0 / (0.3 * uti_depth)
    ''' normalization factor '''
    exp_term = [np.exp(- lam * v) for v in range(1, C + 1)]
    NF = lam * sum(exp_term)
    ''' calculate index '''
    result = lam / NF * np.sum(np.multiply(midPV, exp_term))
    return result


''' cost of round-trip trade of order book'''
''' Ref: liquidity beyond the best quote: A study of the NYSE limit order book '''
def crt(asks, bids, C = 100):
    a_p = [x[0] for x in asks]
    b_p = [x[0] for x in bids]
    ''' capped order size '''
    a_v = [x[1] if x[1] <= C else C for x in asks]
    b_v = [x[1] if x[1] <= C else C for x in bids]
    ''' cumu vol '''
    cumu_a_v = np.cumsum(a_v)
    cumu_b_v = np.cumsum(b_v)
    ''' depth C may be larger than order book total cumulative v '''
    C = min(C, min(cumu_a_v[-1], cumu_b_v[-1]))
    ''' mid '''
    mid = (a_p[0] + b_p[0]) / 2.0
    ''' effective volume '''
    a_ev = []
    b_ev = []
    for i, v in enumerate(cumu_a_v):
        if C >= v:
            a_ev.append(a_v[i])
        elif i > 0:
            a_ev.append(max(0, C - cumu_a_v[i - 1]))
        else:
            a_ev.append(max(0, C))
    for i, v in enumerate(cumu_b_v):
        if C >= v:
            b_ev.append(b_v[i])
        elif i > 0:
            b_ev.append(max(0, C - cumu_b_v[i - 1]))
        else:
            b_ev.append(max(0, C))
    ''' crt '''
    crt = (np.sum(np.multiply(a_ev, np.subtract(a_p, mid))) + np.sum(np.multiply(b_ev, np.subtract(mid, b_p)))) / (C * mid)
    return crt
