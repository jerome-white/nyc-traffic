import csv
import itertools

import numpy as np
import pandas as pd
import lib.window as win
import lib.rollingtools as rt

from lib import logger
from pathlib import Path
from machine import Selector as MachineSelector
from argparse import ArgumentParser
from itertools import islice
from lib.cpoint import Selector as ClassifierSelector
from collections import namedtuple
from lib.network import RoadNetwork
from configparser import ConfigParser
from lib.features import Selector as FeatureSelector
from multiprocessing import Pool

Args = namedtuple('Args', 'segment, data, config')

class Writer:
    def __enter__(self):
        return self.fp

    def __exit__(self, exc_type, exc_value, traceback):
        self.fp.close()
        
    def __init__(self, directory, segment_id, suffix='csv'):
        fname = Path(directory, str(segment_id)).with_suffix('.' + suffix)
        self.fp = open(str(fname), 'w')

class Segment:
    def __init__(self, csv_file, name=None, freq='T'):
        df = pd.read_csv(str(csv_file),
                         index_col='as_of',
                         parse_dates=True,
                         dtype={ x: np.float64 for x in [ 'speed', 'travel' ]})
        
        self.frequency = df.index.to_series().diff().mean().total_seconds()
        self.df = df.resample(freq).mean()
        self.name = name if name is not None else csv_file.stem

    # def lag(self, with_respect_to, multiplier):
    #     lag = self.df.travel.mean() * multiplier
    #     self.df = self.df.shift(round(lag))
    #     self.df.fillna(method='bfill', inplace=True)

class Cluster(list):
    def __init__(self, root):
        super().__init__()
        self.append(root)
        
    def combine(self, interpolate=True):
        reference = self[0].df.index
        
        df = pd.concat([ x.df.speed for x in self ], axis=1)
        df = df.resample(reference.freq).mean()
        df = df.loc[reference.min():reference.max()]
        df.columns = [ x.name for x in self ]
        
        if interpolate:
            df.interpolate(inplace=True)
            for i in 'bf':
                df.fillna(method=i+'fill', inplace=True)
            
        return df

def observe(args):
    log = logger.getlogger()
    log.info('observer {0}'.format(args.data.stem))

    segment = Segment(args.data, name=args.segment)
    if segment.frequency > float(args.config['parameters']['intra-reporting']):
        msg = '{0}: non operational {1}'
        log.error(msg.format(segment.name, segment.frequency))
        return

    #
    # Obtain the network...
    #
    depth = int(args.config['neighbors']['depth'])
    network = RoadNetwork(args.config['data']['network'])
    raw_cluster = list(itertools.islice(network.bfs(segment.name), depth))
    if len(raw_cluster) != depth:
        msg = '{0}: network too shallow {1} {2}'
        log.error(msg.format(segment.name, len(raw_cluster), depth))
        return

    #
    # ... convert it to segments
    #
    path = Path(args.config['data']['raw'])
    cluster = Cluster(segment)
    for (i, level) in enumerate(raw_cluster):
        for j in level:
            p = path.joinpath(str(j)).with_suffix('.csv')
            if not p.exists():
                log.error('No file for {0}'.format(str(p)))
                continue
            s = Segment(p, name=j)
            # s.lag(segment, i)
            cluster.append(s)
    df = cluster.combine()
    
    #
    #
    #
    m = args.config['machine']
    transform = FeatureSelector(m['feature-transform'])()
    threshold = float(args.config['parameters']['acceleration'])
    classifier = ClassifierSelector(m['change-point'])(threshold)
    
    window = win.window_from_config(args.config)
    observations = []
    
    for i in window.slide(df.index):
        label = rt.apply(segment.df.loc[i].values, window, classifier)
        if label is not np.nan:
            features = transform.select(df.loc[i])
            observations.append(features + [ int(label) ])

    #
    #
    #
    with Writer(args.config['data']['observations'], args.segment) as fp:
        writer = csv.writer(fp)
        writer.writerows(observations)
        
def predict(args):
    log = logger.getlogger()
    log.info(args.segment)

    args = args.config['machine']
    observations = np.loadtxt(str(args.data), delimiter=',')
    classifier = MachineSelector(args['model'])(observations)

    predictions = []
    folds = int(args['folds'])
    testing = float(args['testing'])

    for (i, data) in enumerate(classifier.stratify(folds, testing)):
        log.info('fold: {0}'.format(i))
        
        for (name, clf) in classifier.machinate(args['method']):
            try:
                clf.fit(data.x_train, data.y_train)
                pred = clf.predict(data.x_test)
            except (AttributeError, ValueError) as error:
                log.error('{0} {1} {2}'.format(name, data, str(error)))
                continue
            classifier.set_probabilities(clf, data.x_test)

            d = dict(classifier.predict(data, pred))
            d.update({ 'fold': i, 'classifier': name })
            
            predictions.append(d)

    with Writer(args.config['data']['results'], args.segment) as fp:
        writer = csv.DictWriter(fp, predictions[0].keys())
        writer.writeheader()
        writer.writerows(predictions)
        
def enum(config, key):
    path = Path(config['data'][key])
    csv_files = path.glob('*.csv')
    
    yield from map(lambda x: Args(int(x.stem), x, config), csv_files)

############################################################################

arguments = ArgumentParser()
arguments.add_argument('--configuration')
args = arguments.parse_args()
config = ConfigParser()
config.read(args.configuration)

actions = [
    ('raw', observe),
    ('observations', predict),
]

with Pool(1) as pool:
    for (key, func) in filter(lambda x: x[0] in config['data'], actions):
        for _ in pool.imap_unordered(func, enum(config, key)):
            pass
