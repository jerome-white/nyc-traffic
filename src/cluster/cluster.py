import sys
import pickle

import numpy as np
import pandas as pd
import scipy.constants as constant
import matplotlib.pyplot as plt

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
    (index, nid, (window, oneday, threshold)) = args
    
    log.info('{0} create'.format(nid))

    node = nd.Node(nid)
    winlen = nd.winsum(window)

    log.info('{0} apply'.format(nid))

    #
    # determine which windows constitute a traffic event
    #
    df = pd.rolling_apply(node.readings.speed, winlen, g, min_periods=winlen,
                          args=[ window, threshold ])
    df.dropna(inplace=True)

    #
    # aggregate the results
    #
    log.info('{0} aggregate'.format(nid))
    
    vals = []
    if not df.empty:
        totals = OrderedDict(zip(range(oneday), [0] * oneday))
        
        for i in df.index:
            key = cp.bucket(i)
            assert(key in totals)
            totals[key] += df.ix[i]
            
        vals.extend(totals.values())
        vals.append(nid) # this is important
        
    return vals

cargs = cli.CommandLine(cli.optsfile('chgpt'))
args = cargs.args

oneday = round(constant.day / constant.minute)
window = nd.Window(args.window_obs, args.window_pred, args.window_trgt)

if args.resume:
    with open(args.resume, mode='rb') as fp:
        observations = pickle.load(fp)
else:
    opts = [ window, oneday, args.threshold ]
    with Pool() as pool:
        observations = pool.starmap(f, nd.nodegen(opts))
        observations = list(filter(None, observations))
        assert(observations)

    if args.pickle:
        with open(args.pickle, mode='wb') as fp:
            pickle.dump(observations, fp)

# if args.figures and args.verbose:
#     for (nid, tally) in observations:
#         assert(len(tally) == oneday)
#         idx = pd.date_range(start='12:00', periods=oneday, freq='T')
#         df = pd.Series(data=tally, index=idx)
#         elements = [
#             [ '} |', str(nid) ],
#             [ '}', window.observation ],
#             [ '}', window.prediction ],
#             [ '}', window.target ],
#         ]
#         fname = utils.mkfname(args.figures, nid)
#         title = utils.mktitle(elements)
            
#         utils.mkplot(df, fname, title, False, figsize=(12, 8))

if args.clusters > 0:
    (measurements, nodes) = data.cleanse(observations)
    kmeans = KMeans(n_clusters=args.clusters, max_iter=1000, n_init=100,
                    precompute_distances=True, n_jobs=-1)
    kmeans.fit(measurements)

    plt.clf()
    colors = set()
    msize_default = 20 # default size of matplotlib scatter plot marker
    for i in range(args.clusters):
        while True:
            c = utils.hexcolor(white=200)
            if c not in colors:
                colors.add(c)
                break
            
        members = kmeans.labels_ == i
        point_args = {
            'x': measurements[members, 0],
            'y': measurements[members, 1],
            's': msize_default * (3 / 4),
            'marker': '.',
            }
        
        center = kmeans.cluster_centers_[i]
        center_args = {
            'x': center[0],
            'y': center[1],
            's': msize_default * 4,
            'zorder': args.clusters + 1,
            }
        
        for j in (point_args, center_args):
            plt.scatter(c=c, **j)
            
    for i in range(2):
        (l, r) = (measurements[:,i].min() - 1, measurements[:,i].max() + 1)
        (f, g) = (plt.ylim, plt.yticks) if i else (plt.xlim, plt.xticks)
        f(l, r), g(())
    
    fname = utils.mkfname(args.figures, args.clusters)
    plt.savefig(fname)
    plt.close()

    hdr = [ 'node', 'cluster', 'total', 'mean', 'std' ]
    mstats = [ x(measurements, axis=1) for x in (np.sum, np.mean, np.std) ]
    stack = np.dstack((nodes, kmeans.labels_) + tuple(mstats))
    with CSVWriter(hdr) as writer:
        writer.writeheader()
        for row in stack.reshape(-1, stack.shape[-1]):
            line = dict(zip(hdr, row))
            writer.writerow(line)
