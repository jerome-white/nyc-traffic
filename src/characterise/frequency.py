import pickle

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
from collections import namedtuple
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
    df = pd.DataFrame()
        
    for i in loop(window):
        frame = rolling.apply(rt.apply, args=[ i, classifier ])
        aggregate = frame.resample(freq).sum().mean()
        df.set_value(i.prediction, i.target, aggregate)
        
    return rt.NodeData(nid, df)

#############################################################################

log = logger.getlogger(True)

cargs = cli.CommandLine(cli.optsfile('chgpt'))
args = cargs.args

if args.resume:
    with open(args.resume, mode='rb') as fp:
        observations = pickle.load(fp)
else:
    window = Window(args.window_obs, args.window_pred, args.window_trgt)
    
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

head = observations[0].data
(average, variance) = [ pd.DataFrame() for _ in range(2) ]

for i in head.index:
    for j in head.columns:
        vals = [ x.data.loc[i,j] for x in observations ]
        for (a, b) in zip((average, variance), (np.mean, np.std)):
            a.set_value(i, j, b(vals))

#
# Plot
#

PlotInfo = namedtuple('PlotInfo', [ 'name', 'figure', 'labels' ])
plots = [
    PlotInfo('pwin-twin',
             sns.heatmap(average.iloc[::-1], annot=True, fmt='.0f'),
             { 'xlabel': 'Prediction window (minutes)',
               'ylabel': 'Adjacent windows (minutes)',
             }),
    PlotInfo('variance',
             pd.DataFrame(variance.mean(axis=1)).plot(legend=None),
             { 'xlabel': 'Adjacent window (minutes)',
               'ylabel': 'Average standard deviation (minutes)',
             }),
]

for i in plots:
    i.figure.set(**i.labels)
    f = Path(args.figures, i.name).with_suffix('.png')
    utils.mkplot_(i.figure, str(f))
