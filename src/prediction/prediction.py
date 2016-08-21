import numpy as np
import pandas as pd

from pathlib import Path
from argparse import ArgumentParser

class Network(dict):
    def _bfs(self, roots, trail):
        outgoing = []
        for i in filter(lambda x: x in self, roots):
            for j in filter(lambda x: x not in trail, self[i]):
                outgoing.append(j)
        
        if outgoing:
            yield outgoing
            yield from self._bfs(outgoing, trail.union(outgoing))

    def bfs(self, root, inclusive=True):
        roots = [ root ]
        if inclusive:
            yield roots
        yield from self._bfs(roots, set())

class Segment:
    def __init__(self, sid, freq='T'):
        df = pd.read_pickle(db.locate(sid))
        
        self.frequency = df.index.to_series().diff().mean().total_seconds()
        
        self.df = df.resample(freq).mean()
        
        self.speed = self.df.speed
        self.speed.name = sid

    def apply(self, df, window, classifier):
        '''
        determine whether a window constitutes a traffic event
        '''
        assert(type(df) == np.ndarray)

        left_right = []        
        segments = (df[:window.observation], df[-window.target:])
        
        for i in segments:
            if np.isnan(np.sum(i)):
                return np.nan
            left_right.append(i.mean())

        return classifier.classify(window.prediction + 1, *left_right)

    def jams(self, window, classifier):
        roller = self.df.readings.speed.rolling(len(window))
        df = roller.apply(self.apply, args=[ window, classifier ])
        # lag = window.prediction + window.target

        return df.shift(-len(window))

class Cluster(list):
    def group(self, df, window):
        for i in win.idx_range(df.index, size=window.observation):
            yield (i.min(), df.loc[i].values.ravel())

    def combine(self, interpolate=True):
        df = pd.concat([ x.df.speed for x in self ])
        df.columns = [ x.sid for x in self ]
        if interpolate:
            df.interpolate(inplace=True)
        
        return df

def observe(args):
    log = logger.getlogger()
    log(args.segment)
    
    cluster = Cluster()
    path = Path(args.output, args.segment)
    network = Network(args.network_directory)

    for (i, j) in enumerate(islice(network.bfs(args.segment), args.levels)):
        cluster.extend([ Segment(x) for x in j ])
        if i == 0:
            s = cluster[0]
            if s.rate > args.threshold:
                break
            labels = s.jams(args.window, args.classifier)
            labels.dropna(inplace=True)

        log.info('+ combine')
        items = cluster.group(cluster.combine(), args.window)
        df = pd.DataFrame.from_items(items)
        df.merge(labels, how='right', copy=False)
        log.info('- combine {0} {1}'.format(len(df), len(df.columns)))

        assert(not df.isnull().any(axis=1).any())

        fname = '{0:05d}-{1:05d}'.format(i, j)
        p = path.joinpath(fname).with_suffix('.pkl')
        df.to_pickle(str(p))

def predict(args):
    pass

def enum(config):
    path = Path(config['data']['directory'])
    yield from map(lambda x: (x, config), path.iterdir('*.pkl'))

arguments = ArgumentParser()
arguments.add_argument('--configuration')
args = arguments.parse_args()
config = ConfigParser()
config.read(args.configuration)

with Pool() as pool:
    for _ in pool.imap_unordered(func, enum(config)):
        pass
