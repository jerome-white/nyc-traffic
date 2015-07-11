import sys
import utils
import pickle

import node as nd
import numpy as np
import pandas as pd
import cpoint as cp
import scipy.constants as constant

from db import DatabaseConnection
from cli import CommandLine
from data import DataStore
from logger import log
from os.path import expanduser
from csvwriter import CSVWriter
from collections import defaultdict
from multiprocessing import Pool
from sklearn.cluster import KMeans

########################################################################

def aggregate(data, rng, f=sum, default=0):
    l = []
    for i in range(rng):
        if i in data and data[i]:
            entry = f(data[i])
        else:
            log.error('{0} {1}'.format(i in data, bool(data[i])))
            entry = default
        l.append(entry)
        
    return l

# this should live in node.py
def stargen(cli):
    with DatabaseConnection() as conn:
        for (i, j) in enumerate(nd.getnodes(conn)):
            yield (i, j, cli.args)

def f(*args):
    (index, node, cli) = args
     
    log.info(node)
    
    minutes_per_day = round(constant.day / constant.minute)
    oneday = pd.date_range(start=0, periods=minutes_per_day, freq='T')
    win = sum([ cli.window_obs, cli.window_pred, cli.window_trgt ])
    (attempted, completed) = (0, 0) # window accounting
    row = defaultdict(list)

    with DatabaseConnection() as conn:
        source = nd.Node(node, connection=conn)
    
    for period in source.window(win):
        if len(period) < win:
            continue
        attempted += 1
        
        left = period.head(cli.window_obs)
        right = period.tail(cli.window_trgt)

        chg = nd.complete(left) and nd.complete(right)
        if chg:
            (lmean, rmean) = [ i.values.mean() for i in (left, right) ]
            chg = cp.changed(cli.window_pred, lmean, rmean, cli.threshold)
            completed += 1

        key = cp.bucket(right.index[0])
        row[key].append(int(chg))

    if not row:
        log.debug('{0} empty observation'.format(node))
        return []

    # aggregate
    assert(len(row.keys()) == minutes_per_day)
    pct = completed / attempted
    area = sum([ sum(x) for x in row.values() ])
    r = aggregate(row, minutes_per_day)
    # r.extend([ pct, area, node ])
    r.append(node)

    # plot
    if cli.figures:
        key = cli.threshold
        df = pd.DataFrame(columns=[ key ], index=oneday)
        assert(len(df.index) == minutes_per_day)
        df[key] = aggregate(row, len(df.index), np.nanmean)
    
        # df.to_csv(csv)
        elements = [
            [ '} |', str(source) ],
            [ '}', cli.window_obs ],
            [ '}', cli.window_pred ],
            [ '}', cli.window_trgt ],
            [ ':.3f}', pct ],
        ]

        fname = utils.mkfname(cli.figures, node, 'png')
        title = utils.mktitle(elements)
        utils.mkplot(df, fname, title, True)

    return r

########################################################################
    
cli = CommandLine(expanduser('~/.trafficrc/opts.cp'))
args = cli.args

if args.resume:
    with open(args.resume, mode='rb') as fp:
        observations = pickle.load(fp)
else:
    with Pool() as pool:
        observations = list(filter(None, pool.starmap(f, stargen(cli))))
        assert(observations)

    if args.pickle:
        with open(args.pickle, mode='wb') as fp:
            pickle.dump(observations, fp)

data = DataStore(observations, training=1, testing=0)
kmeans = KMeans(n_clusters=args.clusters)
kmeans.fit(data.training.features)

hdr = [ 'node', 'cluster' ]
stack = np.dstack((data.training.labels, kmeans.labels_))
with CSVWriter(hdr) as writer:
    writer.writeheader()
    for row in stack.reshape(-1, stack.shape[-1]):
        line = dict(zip(hdr, row))
        writer.writerow(line)
