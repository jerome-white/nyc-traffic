import pickle

import numpy as np
import pandas as pd
import lib.node as nd
import lib.window as win
import lib.cpoint as cp

from lib import db
from lib import cli
from lib import logger
from pathlib import Path
from configparser import ConfigParser
from multiprocessing import Pool

#############################################################################

def rapply(df, window, classifier):
    '''
    determine whether a window constitutes a traffic event
    '''
    assert(type(df) == np.ndarray)

    segments = (df[:window.target], df[-window.target:])
    (left, right) = [ x.mean() for x in segments ]

    return classifier.change(window.prediction + 1, left, right)
    
def f(args):
    (index, nid, (window, threshold)) = args
    
    log.info('{0} create'.format(nid))

    node = nd.Node(nid)
    classifier = cp.Acceleration(threshold)

    log.info('{0} apply'.format(nid))

    kwargs = {
        'min_periods': len(window),
        'center': True,
        'args': [ window, classifier ]
        }
    df = pd.rolling_apply(node.readings.speed, len(window), rapply, **kwargs)

    log.info('{0} finish'.format(nid))
    
    return (nid, df)

#############################################################################

log = logger.getlogger(True)
log.info('configure')

cargs = cli.CommandLine(cli.optsfile('prediction')) # /etc/opts/prediction

config = ConfigParser()
config.read(cargs.args.config) # --config
params = config['parameters']

# Establish the database credentials. Passing None uses the
# defaults.
dbinfo = config['database'] if 'database' in config else None
db.EstablishCredentials(**dbinfo)
db.genop(int(params['intra-reporting']))

log.info('db version: {0}'.format(db.mark()))

#
# Processing
#
log.info('processing')

with Pool() as pool:
    w = win.from_config(config)
    a = float(params['acceleration'])
    d = { k: v for (k, v) in pool.imap_unordered(f, nd.nodegen(w, a), 1) }
    
log.info('dumping')

pkl = Path(cargs.args.config)
with open(pkl.with_suffix('.pkl'), mode='wb') as fp:
    pickle.dump(d, fp)
