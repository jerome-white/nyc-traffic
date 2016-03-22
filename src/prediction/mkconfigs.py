import itertools
import configparser

from lib import db
from lib import node
from tempfile import NamedTemporaryFile

# http://stackoverflow.com/a/5228294
def product(d):
    for i in itertools.product(*d.values()):
        yield dict(zip(d, i))

# Intra-reporting time must be a single value, across which all runs
# using configuration files generated with this script must obey. This
# is because intra-reporting has a significant impact on the number of
# runs allowed as it dictates the view of the database processes see.
reporting = 120

parallel = False
tmp_params = {
    'mode': 'w',
    'delete': False,
    'prefix': 'nyc.',
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
    if parallel:
        nodes = [ None ]
    else:
        db.genop(reporting) # so that getnodes works properly
        nodes = node.getnodes()
        
    for n in nodes:
        config = configparser.ConfigParser()

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
            'intra-reporting': reporting,
        }
        if n is not None:
            config[p]['node'] = str(n)

        with NamedTemporaryFile(**tmp_params) as fp:
            print(fp.name)
            config.write(fp)

