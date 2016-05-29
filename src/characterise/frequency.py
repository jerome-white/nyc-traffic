import pickle
import collections

import numpy as np
import pandas as pd
import seaborn as sns
import lib.cpoint as cp
import rollingtools as rt
import scipy.constants as constant
import matplotlib.pyplot as plt

from lib import db
from lib import cli
from lib import ngen
from lib import utils
from lib import logger
from lib import node as nd
from pathlib import Path
from lib.window import Window
from multiprocessing import Pool

#############################################################################

def loop(window):
    for p in range(1, window.prediction + 1):
        for t in range(1, window.target + 1):
            yield Window(t, p, t)
    
def f(*args):
    (index, nid, (window, threshold, freq)) = args
    
    log.info('{0} create'.format(nid))

    classifier = cp.Acceleration(threshold)
    readings = nd.Node(nid).readings.speed
    rolling = readings.rolling(len(window), center=True)

    df = df.DataFrame(data=np.zeros((window.prediction, window.target)))
    for i in [ df.index, df.columns ]:
        i += 1
        
    for i in loop(window):
        frame = rolling.apply(rt.apply, args=[ i, classifier ])
        log.info('{0} {1} {2} {3}'.format(nid, i, frame.sum(), frame.count()))

        aggregate = frame.resample(freq).sum()
        df.set_value(i.prediction, i.target, aggregate)
        
    return df

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
    # XXX Must establish credentials!
    g = ngen.SequentialGenerator().nodegen
    
    with Pool() as pool:
        observations = pool.starmap(f, g(window, args.threshold, 'D'))
        
    if args.pickle:
        with open(args.pickle, mode='wb') as fp:
            pickle.dump(observations, fp)

#
# Collect the data
#

head = observations[0]
(average, variance) = [ pd.DataFrame() for _ in range(2) ]

for i in head.index:
    for j in head.columns:
        vals = [ x.loc[i,j] for x in observations ]
        for (a, b) in [ (average, np.mean), (variance, np.std) ]:
            a.set_value(i, j, b(vals))

#
# Plot
#

plots = [
    { 'name': 'pwin-twin',
      'figure': sns.heatmap(average, annot=True),
      'labels': { 'xlabel': 'Prediction window',
                  'ylabel': 'Adjacent windows',
      },
    },
    { 'name': 'variance',
      'figure': variance.plot(),
      'labels': { 'xlabel': 'Window size',
                  'ylabel': 'Std. Deviation',
      },
    },
]

for i in plots:
    heatplot(i['figure'], args.figures, i['name'], i['labels'])
