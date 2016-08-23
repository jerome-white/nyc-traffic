#
# Generate the configuration file(s) used by the predictor.
#

import itertools
import configparser

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
# arguments.add_argument('--parallel')
arguments.add_argument('--verbose')
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
    'test-train': [],
    'observation': [ str(x) for x in [ 5 ] ],
    'prediction': [ str(x) for x in [ 3 ] ],
    'target': [ str(x) for x in [ 5 ] ],
    'depth': [ str(x) for x in range(8) ],
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
    'window': window.names_,
    'neighbors': [ 'depth', 'selection' ],
}
for (i, o) in enumerate(product(options)):
    config = configparser.ConfigParser()
    (test, train) =  o['test-train'].split(',')

    for (title, keys) in helper.items():
        config[title] = { k: o[k] for k in keys }
        
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

    with NamedTemporaryFile(**tmpargs) as fp:
        if args.verbose:
            print(fp.name)
        config.write(fp)
