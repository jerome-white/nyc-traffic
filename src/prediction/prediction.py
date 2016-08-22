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
    (data, config) = args
    
    log = logger.getlogger()
    log(data.stem)

    root = pd.from_csv(data)
    rate = root.speed.index.to_series().diff().mean().total_seconds()
    if rate > float(config['parameters']['intra-reporting']):
        log.error('{0}: {1}', root.name, rate)
        return

    #
    # Obtain the network...
    #
    depth = int(config['neighbors']['depth'])
    network = Network(config['data']['network'])
    raw_cluster = list(itertools.islice(network.bfs(root.name), depth))
    if len(raw_cluster) != depth + 1:
        log.error('{0}: {1} {2}'.format(root.name, len(raw_cluster), depth))
        return

    #
    # ... convert it to segments
    #
    path = Path(config['data']['raw'])
    segments = [ root ]
    for (i, level) in enumerate(raw_cluster):
        for j in level:
            p = path.joinpath(j).with_suffix('.csv')
            df = pd.from_csv(str(p))

            lag = df.travel.mean() * i
            df = df.shift(round(lag))
            df.fillna(method='bfill', inplace=True)
        
            segments.append(df.speed)
    cluster = pd.concat(segments)
    
    #
    #
    #
    missing = root[root.isnull()].index
    cluster.interpolate(inplace=True)

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
    output = Path(config['data']['observations'], segment_id)
    output = output.with_suffix('.csv')
    with open(str(output)) as fp:
        writer = csv.writer(fp)
        writer.writerows(observations)

def predict(args):
    (data, config) = args

    observations = np.loadtxt(data, delimiter=',')

def enum(config, key):
    path = Path(config['data'][key])
    yield from map(lambda x: (x, config), path.iterdir('*.csv'))

############################################################################

arguments = ArgumentParser()
arguments.add_argument('--configuration')
args = arguments.parse_args()
config = ConfigParser()
config.read(args.configuration)

l = OrderedDict([
        ('raw', observe),
        ('observations', predict),
])

with Pool() as pool:
    for (key, f) in l.items():
        for _ in pool.imap_unordered(f, enum(config, key)):
            pass
