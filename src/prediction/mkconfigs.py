#
# Generate the configuration file(s) used by the predictor.
#

import itertools
import configparser
from uuid import uuid4
from pathlib import Path
from argparse import ArgumentParser

import numpy as np

from lib import window

# http://stackoverflow.com/a/5228294
def product(d):
    for i in itertools.product(*d.values()):
        yield dict(zip(d, i))

arguments = ArgumentParser()
arguments.add_argument('--data')
arguments.add_argument('--network')
arguments.add_argument('--top-level')
arguments.add_argument('--reporting-threshold')
args = arguments.parse_args()

#
# Options that can be simultaneous during a single run
#
machines = [
    # svm
    # bayes
    'forest',
    # tree
    # dummy 
]

#
# Options that are singluar (only a single value can be active during
# a run)
#
options = {
    'transform': [
        'simple',
        # 'change',
        # 'average',
        # 'difference',
    ],
    'change': [
        # 'percentage',
        # 'derivative',
        'acceleration',
    ],
    'selection': [
        'simple',
        # 'var',
        # 'hybrid',
    ],
    'observation': np.linspace(3, 9, 3, dtype=int),
    'prediction': np.linspace(3, 9, 3, dtype=int),
    # 'target': [ str(x) for x in [ 5 ] ],
    'depth': np.linspace(0, 5, 6, dtype=int),
    'test-train': [],
}

testing_sizes = [ 0.2 ]
for i in map(lambda x: (x, 1 - x), testing_sizes):
    j = ','.join(map(str, i))
    options['test-train'].append(j)

#
# Build the file!
#
helper = {
    'window': list(filter(lambda x: x in options, window.names_)),
    'neighbors': [ 'depth', 'selection' ],
}
for (i, o) in enumerate(product(options)):
    config = configparser.ConfigParser()
    (test, train) =  o['test-train'].split(',')

    for (title, keys) in helper.items():
        config[title] = { k: str(o[k]) for k in keys }
        
    config['machine'] = {
        'folds': str(10),
        'model': 'classification',
        'method': ','.join(machines),
        'feature-transform': o['transform'],
        'change-point': o['change'],
        'testing': test,
    }

    config['parameters'] = {
        'acceleration': str(-0.002),
    }
    if args.reporting_threshold:
        config['parameters']['intra-reporting'] = args.reporting_threshold

    config['data'] = {
        'raw': args.data,
        'network': args.network,
    }

    while True:
        (c, _) = str(uuid4()).split('-', 1)
        path = Path(args.top_level, c)
        try:
            path.mkdir(parents=True)
            break
        except FileExistsError:
            pass

    path = path.joinpath('ini')
    with path.open('w') as fp:
        config.write(fp)
