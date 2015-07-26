import sys
import pickle

import numpy as np
import pandas as pd
import scipy.constants as constant

from collections import OrderedDict
from multiprocessing import Pool
from sklearn.cluster import KMeans

from lib import cli
from lib import utils
from lib import data
from lib import node as nd
from lib import cpoint as cp
from lib.db import DatabaseConnection
from lib.logger import log
from lib.csvwriter import CSVWriter

def g(df, *args):
    assert(type(df) == np.ndarray)
    
    (window, threshold) = args

    # left = df.head(window.observation).mean()
    # right = df.tail(window.target).mean()

    segments = (df[:window.observation], df[-window.target:])
    (left, right) = [ np.mean(x) for x in segments ]
    
    return cp.changed(window.prediction, left, right, threshold)
    
def f(*args):
    (index, nid, opts) = args

    oneday = round(constant.day / constant.minute)
    totals = OrderedDict(zip(range(oneday), [0] * oneday))
    
    log.info('{0} create'.format(nid))

    node = nd.Node(nid)
    window = nd.Window(opts.window_obs, opts.window_pred, opts.window_trgt)
    winlen = nd.winsum(window)

    log.info('{0} apply'.format(nid))

    df = pd.rolling_apply(node.readings.speed, winlen, g, min_periods=winlen,
                          center=True, args=[ window, opts.threshold ])
    df.dropna(inplace=True)
    if not df.empty:
        log.info('{0} aggregate'.format(nid))
    
        for i in df.index:
            key = cp.bucket(i)
            assert(key in totals)
            totals[key] += df.ix[i]

        if opts.figures:
            log.info('{0} plot'.format(nid))

            idx = pd.date_range(start='12:00', periods=oneday, freq='T')
            df = pd.Series(data=totals, index=idx)
            elements = [
                [ '} |', str(node) ],
                [ '}', window.observation ],
                [ '}', window.prediction ],
                [ '}', window.target ],
                ]
            fname = utils.mkfname(opts.figures, node.nid)
            title = utils.mktitle(elements)
        
            utils.mkplot(df, fname, title, False, kind='bar')

    return list(totals.values())

cargs = cli.CommandLine(cli.optsfile('chgpt'))
args = cargs.args

if args.resume:
    with open(args.resume, mode='rb') as fp:
        observations = pickle.load(fp)
else:
    with Pool() as pool:
        observations = pool.starmap(f, nd.nodegen(args))
        observations = list(filter(None, observations))
        assert(observations)

    if args.pickle:
        with open(args.pickle, mode='wb') as fp:
            pickle.dump(observations, fp)

if args.clusters > 0:
    (features, labels) = data.cleanse(observations)
    kmeans = KMeans(n_clusters=args.clusters)
    kmeans.fit(features)

    hdr = [ 'node', 'cluster' ]
    stack = np.dstack((labels, kmeans.labels_))
    with CSVWriter(hdr) as writer:
        writer.writeheader()
        for row in stack.reshape(-1, stack.shape[-1]):
            line = dict(zip(hdr, row))
            writer.writerow(line)
