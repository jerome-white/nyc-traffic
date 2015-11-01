import pickle

import numpy as np
import pandas as pd
import seaborn as sns
import scipy.constants as constant
import matplotlib.pyplot as plt

from lib import db
from lib import cli
from lib import utils
# from lib import data
from lib import node as nd
from pathlib import Path
from lib.logger import log
from lib.window import Window
from multiprocessing import Pool

class Aggregate:
    def collect(self, df):
        raise NotImplementedError()

    def reduce(self, lst):
        raise NotImplementedError()

class Count(Aggregate):
    def collect(self, df):
        return df.sum()

    def reduce(self, lst):
        return sum(lst)
    
class PerDay(Aggregate):
    def collect(self, df):
        rng = df.index[-1] - df.index[0]
        days = rng.total_seconds() / constant.day
    
        return df.sum() / days

    def reduce(self, lst):
        return np.mean(lst)

#############################################################################

def loop(window):
    for i in range(window.prediction):
        for j in range(1, window.target):
            yield Window(j, i, j)
    
def g(df, *args):
    assert(type(df) == np.ndarray)

    (window, threshold) = args

    segments = (df[:window.target], df[-window.target:])
    if np.isnan(segments).any():
        return np.nan
    (left, right) = [ x.mean() for x in segments ]
    change = (right - left) / left
    
    return change <= threshold
    
def f(*args):
    (index, nid, (window, threshold, agg)) = args
    
    log.info('{0} create'.format(nid))

    node = nd.Node(nid)
    events = np.zeros((window.prediction, window.target))

    #
    # determine which windows constitute a traffic event
    #
    for i in loop(window):
        args = (i, threshold)
        df = pd.rolling_apply(node.readings.speed, len(i), g, args=args)
        df.dropna(inplace=True)

        log.info('{0}: {1} {2}/{3}'.format(nid, i, df.sum(), df.count()))
            
        events[i.prediction][i.target] = agg(df)
        
    return events

#############################################################################

cargs = cli.CommandLine(cli.optsfile('chgpt'))
args = cargs.args

window = Window(args.window_obs, args.window_pred, args.window_trgt)
aggregate = PerDay()

if args.resume:
    with open(args.resume, mode='rb') as fp:
        observations = pickle.load(fp)
else:
    db.genop(args.reporting)

    opts = [ window, args.threshold, aggregate.collect ]
    with Pool() as pool:
        observations = pool.starmap(f, nd.nodegen(opts))
        observations = list(filter(lambda x: x.size > 0, observations))
        assert(observations)

    if args.pickle:
        with open(args.pickle, mode='wb') as fp:
            pickle.dump(observations, fp)

processed = np.zeros((window.prediction, window.target))
for i in loop(window):
    row = [ x[i.prediction][i.target] for x in observations ]
    assert(processed[i.prediction][i.target] == 0)
    processed[i.prediction][i.target] = aggregate.reduce(row)
processed = np.delete(processed, 0, axis=1)
    
# kwargs = { 'ylabel': 'Prediction window', 'xlabel': 'Target window' }
fig = sns.heatmap(processed) # , kwargs=kwargs)
fname = Path(args.figures, 'pwin-twin').with_suffix('.pdf')
utils.mkplot_(fig, str(fname))
        
