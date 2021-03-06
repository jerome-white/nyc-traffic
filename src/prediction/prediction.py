import csv
from pathlib import Path
from argparse import ArgumentParser
from itertools import islice
from collections import namedtuple
from configparser import ConfigParser
from multiprocessing import Pool

import numpy as np
import pandas as pd

import lib.rollingtools as rt
from lib import logger
from lib.window import window_from_config
from lib.cpoint import Selector as ClassifierSelector
from lib.network import RoadNetwork
from lib.features import Selector as FeatureSelector

import ledger
from machine import Selector as MachineSelector

Entry = namedtuple('Entry', 'ini, segment, event')
Args = namedtuple('Args', 'segment, data, root, entry, config, cli')

class Writer:
    def __enter__(self):
        return self.fp

    def __exit__(self, exc_type, exc_value, traceback):
        self.fp.close()
        
    def __init__(self, directory, subdirectory, segment_id, suffix='csv'):
        path = Path(directory, subdirectory)
        path.mkdir(parents=True, exist_ok=True)
        fname = path.joinpath(str(segment_id)).with_suffix('.' + suffix)

        self.fp = fname.open('w')

def observe(args):
    log = logger.getlogger()
    log.info('+ {0}: observe'.format(args.segment))

    segment = Segment(args.data, name=args.segment)

    if 'intra-reporting' in args.config['parameters']:
        frequency = float(args.config['parameters']['intra-reporting'])
        if segment.frequency > frequency:
            msg = '{0}: non operational {1}'
            log.error(msg.format(segment, segment.frequency))
            return (args.entry, False)

    #
    # Obtain the network...
    #
    depth = int(args.config['neighbors']['depth'])
    network = RoadNetwork(args.cli.network)
    raw_cluster = list(islice(network.bfs(segment.name), depth))
    if len(raw_cluster) != depth:
        msg = '{0}: network too shallow {1} {2}'
        log.error(msg.format(segment, len(raw_cluster), depth))
        return (args.entry, False)

    #
    # ... convert it to segments
    #
    cluster = Cluster(segment)
    for (i, level) in enumerate(raw_cluster):
        for j in level:
            p = args.cli.data.joinpath(str(j)).with_suffix('.csv')
            if not p.exists():
                log.warning('No file for {0}'.format(str(p)))
                continue
            s = Segment(p, name=j)
            # s.lag(segment, i)
            cluster.append(s)
    df = cluster.combine()
    
    #
    # Setup machine parameters
    #
    m = args.config['machine']
    transform = FeatureSelector(m['feature-transform'])()
    threshold = float(args.config['parameters']['acceleration'])
    classifier = ClassifierSelector(m['change-point'])(threshold)
    
    window = window_from_config(args.config)
    observations = []

    log.debug('+ {0}: slide'.format(args.segment))
    for i in window.slide(df.index):
        label = rt.apply(segment.df.loc[i].values, window, classifier)
        if label is not np.nan:
            index = i[:window.observation]
            features = transform.select(df.loc[index])
            observations.append(features + [ int(label) ])
    log.debug('- {0}: slide'.format(args.segment))

    if not observations:
        log.error('{0}: No observations'.format(segment))
        return (args.entry, False)
    
    #
    # Save observations to disk
    #
    with Writer(args.root, 'observations', args.segment) as fp:
        writer = csv.writer(fp)
        writer.writerows(observations)

    return (args.entry, True)
        
def predict(args):
    log = logger.getlogger()
    log.info('+ {0}: predict'.format(args.segment))

    machine_opts = args.config['machine']
    
    #
    # Obtain the observations
    #
    path = args.root.joinpath('observations', str(args.segment))
    results = path.with_suffix('.csv')
    if not results.exists():
        return (args.entry, False)
    observations = np.loadtxt(str(results), delimiter=',')
    if observations.ndim < 2:
        return (args.entry, False)
    classifier = MachineSelector(machine_opts['model'])(observations)

    segment = Segment(args.data, name=args.segment)

    predictions = []
    folds = int(machine_opts['folds'])
    testing = float(machine_opts['testing'])

    #
    # Make the predictions
    #
    log.debug('+ {0}: stratify'.format(args.segment))
    for (i, data) in enumerate(classifier.stratify(folds, testing)):
        log.debug('fold: {0}'.format(i))
        
        for (name, clf) in classifier.machinate(machine_opts['method']):
            try:
                clf.fit(data.x_train, data.y_train)
                pred = clf.predict(data.x_test)
            except (AttributeError, ValueError) as error:
                log.warning('{0} {1} {2}'.format(name, data, str(error)))
                continue
            classifier.set_probabilities(clf, data.x_test)

            d = dict(classifier.predict(data, pred))
            d.update({ 'fold': i,
                       'classifier': name,
                       'frequency': segment.frequency,
                       'segment': str(segment),
            })
            predictions.append(d)
    log.debug('- {0}: stratify'.format(args.segment))

    if not predictions:
        return (args.entry, False)
    
    #
    # Save results to disk
    #
    with Writer(args.root, 'results', args.segment) as fp:
        writer = csv.DictWriter(fp, predictions[0].keys())
        writer.writeheader()
        writer.writerows(predictions)

    return (args.entry, True)

def enumerator(records, event, cli):
    csv_files = sorted(cli.data.glob('*.csv'))

    for data_file in islice(csv_files, cli.node, None, cli.total_nodes):
        segment_id = int(data_file.stem)
    
        for run_dir in cli.top_level.iterdir():
            ini = run_dir.joinpath('ini')
            if not ini.is_file():
                continue

            entry = ledger.Entry(run_dir.stem, segment_id, event)
            if entry in records:
                continue

            config = ConfigParser()
            config.read(str(ini))

            yield Args(segment_id, data_file, run_dir, entry, config, cli)

############################################################################

arguments = ArgumentParser()
arguments.add_argument('--data', type=Path)
arguments.add_argument('--network', type=Path)
arguments.add_argument('--top-level', type=Path)
arguments.add_argument('--observe', action='store_true')
arguments.add_argument('--predict', action='store_true')
arguments.add_argument('--node', type=int, default=0)
arguments.add_argument('--total-nodes', type=int, default=1)
args = arguments.parse_args()

log = logger.getlogger(True)

actions = [
    (args.observe, observe),
    (args.predict, predict),
]
ldir = args.top_level.joinpath('.ledger')

log.info('|> {0}/{1}'.format(args.node, args.total_nodes))
with ledger.Ledger(ldir, args.node) as records:
    with Pool(maxtasksperchild=1) as pool:
        for (_, func) in filter(all, actions):
            f = func.__name__
            for i in pool.imap_unordered(func, enumerator(records, f, args)):
                records.record(*i)
log.info('|< {0}/{1}'.format(args.node, args.total_nodes))
