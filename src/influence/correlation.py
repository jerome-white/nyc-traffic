import pickle

import numpy as np
import pandas as pd
import datetime as dt
import scipy.stats as st

from os.path import expanduser
from tempfile import NamedTemporaryFile
from multiprocessing import Pool

from lib import cli
from lib import node as nd
from lib.db import DatabaseConnection
from lib.logger import Logger
from lib.csvwriter import CSVWriter

class Influence:
    def __init__(self, source, neighbors, args):
        self.source = source
        self.neighbors = neighbors
        self.args = args
        self.log = Logger().log

    def inner(self, source, target):
        return [ [] for i in range(3) ]
    
    def run(self):
        values = []
                    
        for target in self.neighbors:
            msg = '{0}: {1}->{2}'
            self.log.info(msg.format(repr(self),self.source.node,target.node))
            
            (left, right, delay) = self.inner(self.source, target)
            d = {
                'type': repr(self),
                'source': self.source.node,
                'target': target.node,
                'pearson': st.pearsonr(left, right),
                'spearman': st.spearmanr(left, right),
                'delay': np.mean(delay),
                }
            values.append(d)

        return values

    def __repr__(self):
        return type(self).__name__
    
class MinuteInfluence(Influence):
    def get(self, node, index):
        df = node.readings
        return [ df.ix[index,x].item() for x in df.columns ]
    
    def inner(self, source, target):
        (left, right, delay) = super().inner(source, target)
        
        for i in target.readings.index:
            readings = self.get(target, i)
            if np.all(np.isfinite(readings)):
                (tx, ty) = readings
                dly = round(ty)
                k = i + dt.timedelta(minutes=dly)
                try:
                    (sx, _) = self.get(source, k)
                    if np.isfinite(sx):
                        left.append(sx)
                        right.append(tx)
                        delay.append(dly)
                except KeyError:
                    pass
                
        return (left, right, delay)

class WindowInfluence(Influence):
    def __init__(self, source, neighbors, args):
        super().__init__(source, neighbors, args)
        
        self.window = nd.Window(args.window_obs,
                                args.window_pred,
                                args.window_trgt)
        
    def inner_(self, source, target):
        (left, right, delay) = super().inner(source, target)
        
        for i in target.readings.index:
            rng = pd.date_range(i, periods=self.window.observation,
                                freq=target.freq)
            r = target.readings.loc[rng]
            if nd.complete(r):
                dly = round(r.travel.mean())
                l = source.readings.loc[rng.shift(dly)]
                if nd.complete(l):
                    delay.append(dly)
                    for (x, y) in zip((left, right), (l, r)):
                        x.append(y.speed.mean())
                
        return (left, right, delay)
    
    def inner(self, source, target):
        (left, right, delay) = super().inner(source, target)
        
        for (i, _) in target.range(self.window):
            r = target.readings.ix[i]
            if nd.complete(r):
                dly = round(r.travel.mean())
                j = i.shift(dly)
                l = source.readings.ix[j]
                if nd.complete(l):
                    delay.append(dly)
                    for (x, y) in zip((left, right), (l, r)):
                        x.append(y.speed.mean())
        
        return (left, right, delay)
            
def stargen(cargs):
    with DatabaseConnection() as conn:
        for (i, node) in enumerate(nd.getnodes(conn)):
            yield (i, node, cargs.args)
                    
def f(*args):
    (_, node, cargs) = args
    log = Logger().log

    log.info('{0}: setup +'.format(node))
    with DatabaseConnection() as conn:
        source = nd.Node(node, conn)
        neighbors = [ nd.Node(x, conn) for x in source.neighbors ]
    log.info('{0}: setup -'.format(node))
    
    classes = [ WindowInfluence ] # [ MinuteInfluence, WindowInfluence ]
    
    return [ i(source, neighbors, cargs).run() for i in classes ]

with Pool() as pool:
    cargs = cli.CommandLine(cli.optsfile('main'))
    
    results = pool.starmap(f, stargen(cargs))
    with NamedTemporaryFile(mode='wb', delete=False) as fp:
        pickle.dump(results, fp)
        msg = 'pickle: {0}'.format(fp.name)
        Logger().log.error(msg)

# with open('/tmp/tmpe2x8wi0d', mode='rb') as fp:
#     results = pickle.load(fp)
    
header = [
    'type',
    'source',
    'target',
    'pearson',
    'spearman',
    'delay',
]
with CSVWriter(header, delimiter=';') as writer:
    writer.writeheader()
    for i in filter(None, results):
        for j in i:
            writer.writerows(j)
