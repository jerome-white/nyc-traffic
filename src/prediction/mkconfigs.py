#
# Generate the configuration file(s) used by the predictor.
#

import itertools
import configparser

import numpy as np

from lib import db
from lib import cli
from lib import node
from lib import window
from argparse import ArgumentParser
from tempfile import NamedTemporaryFile

# http://stackoverflow.com/a/5228294
def product(d):
    for i in itertools.product(*d.values()):
        yield dict(zip(d, i))

arguments = ArgumentParser()
arguments.add_argument('--reporting-threshold')
arguments.add_argument('--output-directory')
arguments.add_argument('--data')
arguments.add_argument('--network')
args = arguments.parse_args()

tmpargs = {
    'mode': 'w',
    'delete': False,
    'dir': args.output_directory,
    'prefix': '', # empty string defaults to 'tmp'
    'suffix': '.ini',
}

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
p = 'parameters'
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

    config[p] = {
        'acceleration': str(-0.002),
        'intra-reporting': args.reporting_threshold,
    }

    config['data'] = {
        'raw': args.data,
        'network': args.network,
    }

    while True:
        (c, _) = str(uuid4()).split('-', 1)
        path = Path(args.output_directory, c)
        try:
            path.mkdir(parents=True)
            break
        except FileExistsError:
            pass

    path = path.joinpath('ini')
    with path.open() as fp:
        config.write(fp)
