import itertools
import configparser

from lib import db
from lib import node
from tempfile import NamedTemporaryFile

# http://stackoverflow.com/a/5228294
def product(d):
    for i in itertools.product(*d.values()):
        yield dict(zip(d, i))

def add(source, target, key, indices):
    if key not in target:
        target[key] = {}
        
    for i in indices:
        target[key][i] = source[i]

reporting = 120
tmp_params = {
    'mode': 'w',
    'delete': False,
    'prefix': 'nyc.',
    'suffix': '.cfg',
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
for (i, o) in enumerate(product(options)):
    db.genop(reporting)
    for n in node.getnodes():
        config = configparser.ConfigParser()

        add(o, config, 'window', [ 'observation', 'prediction', 'target' ])
        add(o, config, 'neighbors', [ 'depth', 'selection' ])
        
        config['machine'] = {
            'folds': str(10),
            'model': 'classification',
            'method': ','.join(machines),
            'feature-transform': o['transform'],
        }

        config['output'] = {
            'print-header': str(i == 0),
        }

        config['parameters'] = {
            'acceleration': str(-0.002),
            'intra-reporting': reporting,
            
            # Having a node value implicitly forces sequential operation
            'node': str(n),
        }

        with NamedTemporaryFile(**tmp_params) as fp:
            print(fp.name)
            config.write(fp)
