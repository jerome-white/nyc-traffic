#
# Generate the configuration file(s) used by the predictor.
#

import itertools
import configparser

from lib import db
from lib import cli
from lib import node
from tempfile import NamedTemporaryFile

# http://stackoverflow.com/a/5228294
def product(d):
    for i in itertools.product(*d.values()):
        yield dict(zip(d, i))

cargs = cli.CommandLine(cli.optsfile('config')) # /etc/opts/config
args = cargs.args

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
    'selection': [
        'simple',
        # 'var',
        # 'hybrid',
    ],
    'observation': [ str(x) for x in [ 12 ] ],
    'prediction': [ str(x) for x in [ 3 ] ],
    'target': [ str(x) for x in [ 8 ] ],
    'depth':  [ str(x) for x in range(8) ],
}

#
# Build the file!
#
p = 'parameters'
helper = {
    'window': [ 'observation', 'prediction', 'target' ],
    'neighbors': [ 'depth', 'selection' ],
}
for (i, o) in enumerate(product(options)):
    if args.parallel:
        nodes = [ None ]
        if not i:
            db.genop(args.reporting)
    else:
        db.genop(args.reporting)
        nodes = node.getnodes()
    
    for n in nodes:
        config = configparser.ConfigParser()
        if args.skeleton:
            config.read(args.skeleton)

        for (title, keys) in helper.items():
            config[title] = { k: o[k] for k in keys }
        
        config['machine'] = {
            'folds': str(10),
            'model': 'classification',
            'method': ','.join(machines),
            'feature-transform': o['transform'],
        }

        config['output'] = {
            'print-header': str(i == 0),
        }

        config[p] = {
            'acceleration': str(-0.002),
            'intra-reporting': args.reporting,
        }
        if n is not None:
            config[p]['node'] = str(n)

        with NamedTemporaryFile(mode='w', delete=False, dir=args.output) as fp:
            if args.verbose:
                print(fp.name)
            config.write(fp)
