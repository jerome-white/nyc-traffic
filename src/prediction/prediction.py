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
    (segment_id, config) = args
    
    log = logger.getlogger()
    log(args.segment)

    #
    # Obtain the network...
    #
    depth = int(config['neighbors']['depth'])
    network = Network(config['data']['network'])
    raw_cluster = list(itertools.islice(network.bfs(args.segment), depth))
    if len(raw_cluster) != depth + 1:
        log.error('{0}: {1} {2}'.format(segment_id, len(segments), depth))
        return

    #
    # ... convert it to segments
    #
    path = Path(config['data']['raw'])
    segments = []
    columns = []
    for (i, j) in enumerate(raw_cluster):
        for k in j:
            p = path.joinpath(k).with_suffix('.pkl')
            df = pd.from_pickle(str(p))
            if i == 0:
                rate = df.index.to_series().diff().mean().total_seconds()
                if rate > float(config['parameters']['intra-reporting']):
                    log.error('{0}: {1}', segment_id, rate)
                    return
            
            lag = df.travel.mean() * i
            df = df.shift(round(lag))
            df.fillna(method='bfill', inplace=True)
        
            segments.append(df.speed)
    cluster = pd.concat(segments)
    cluster.columns = segment_list

    root = cluster[segment_id]
    missing = root[root.isnull()].index
    cluster.interpolate(inplace=True)
    
    #
    #
    #
    observations = []
    window = win.window_from_config(config)
    for i in window.slide(cluster.index):
        label = rt.apply(root[i], window, classifier)
        if label is np.nan:
            continue

        features = cluster.loc[i].values.ravel().tolist()
        observations.append(features + [ label ])

    #
    #
    #
    with open(config['data']['observations']) as fp:
        writer = csv.writer(fp)
        writer.writerows(observations)

def predict(args):
    pass

def enum(config):
    path = Path(config['data']['raw'])
    yield from map(lambda x: (int(x.stem), config), path.iterdir('*.pkl')

arguments = ArgumentParser()
arguments.add_argument('--configuration')
args = arguments.parse_args()
config = ConfigParser()
config.read(args.configuration)

with Pool() as pool:
    for f in [ observe, predict ]:
        for _ in pool.imap_unordered(f, enum(config)):
            pass
