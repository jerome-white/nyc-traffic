import pickle
import collections

import numpy as np
import pandas as pd
import seaborn as sns
import lib.cpoint as cp
import scipy.constants as constant
import matplotlib.pyplot as plt

from lib import db
from lib import cli
from lib import utils
from lib import logger
# from lib import data
from lib import node as nd
from pathlib import Path
from lib.window import Window
from multiprocessing import Pool

#############################################################################

def loop(window):
    for i in range(window.prediction):
        for j in range(1, window.target):
            yield Window(j, i, j)

def rapply(df, window, classifier):
    '''
    determine whether a window constitutes a traffic event
    '''
    assert(type(df) == np.ndarray)

    segments = (df[:window.target], df[-window.target:])
    if np.isnan(segments).any():
        return np.nan
    (left, right) = [ x.mean() for x in segments ]

    return classifier.classify(window.prediction + 1, left, right)
    
def f(*args):
    (index, nid, (window, threshold, freq)) = args
    
    log.info('{0} create'.format(nid))

    classifier = cp.Acceleration(threshold)
    readings = nd.Node(nid).readings.speed
    stats = [ [ 0 ] * window.target for _ in range(window.prediction) ]

    for i in loop(window):
        rolling = readings.rolling(len(window), min_periods=len(window),
                                   center=True)
        df = rolling.apply(rapply, args=[ i, classifier ])
        log.info('{0} {1} {2} {3}'.format(nid, i, df.sum(), df.count()))
        stats[i.prediction][i.target] = df.resample(freq, how=sum)
        
    return stats

# def g(*args):
#     (prediction, target, data, aggregate) = args
    
#     return aggregate(data[prediction][target])

def var2std(df):
    d = pd.DataFrame()
    for (i, j) in enumerate([ 'Target', 'Prediction' ]):
        d[j] = np.sqrt(df.sum(i))
        
    return d

def heatplot(fig, directory, fname, labels, extension='png'):
    fig.set(**labels)
    f = Path(args.figures, fname).with_suffix('.' + extension)
    utils.mkplot_(fig, str(f))

#############################################################################

log = logger.getlogger(True)

cargs = cli.CommandLine(cli.optsfile('chgpt'))
args = cargs.args
window = Window(args.window_obs, args.window_pred, args.window_trgt)

if args.resume:
    with open(args.resume, mode='rb') as fp:
        observations = pickle.load(fp)
else:
    # dbinfo = config['database'] if 'database' in config else None
    # db.EstablishCredentials(**dbinfo)
    db.genop(args.reporting)
    with Pool() as pool:
        observations = pool.starmap(f, nd.nodegen(window, args.threshold, 'D'))
        
    if args.pickle:
        with open(args.pickle, mode='wb') as fp:
            pickle.dump(observations, fp)

#
# Collect the data
#

data = np.zeros((window.prediction, window.target))
columns = list(range(window.target))
df_mean = pd.DataFrame(data=data.copy(), columns=columns)
df_var = pd.DataFrame(data=data.copy(), columns=columns)

for w in loop(window):
    (_, i, j) = w
    row = [ x[i][j].mean() for x in observations ]
    df_mean.loc[i][j] = np.mean(row)
    df_var.loc[i][j] = np.var(row)
#    print(w, np.mean(row), '\n', df_mean, '\n')
    
for i in (df_mean, df_var):
    i.drop(0, axis=1, inplace=True)

#
# Plot
#

plots = [
    { 'name': 'pwin-twin',
      'figure': sns.heatmap(df_mean.iloc[::-1], annot=True),
      'labels': { 'xlabel': 'Target window', 'ylabel': 'Prediction window' },
    },
    { 'name': 'variance',
      'figure': var2std(df_var).plot(),
      'labels': { 'xlabel': 'Window size', 'ylabel': 'Std. Deviation' },
    },
]

for i in plots:
    heatplot(i['figure'], args.figures, i['name'], i['labels'])
