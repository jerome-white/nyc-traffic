import pickle
import itertools

import pandas as pd
import lib.node as nd
import lib.window as win
import lib.cpoint as cp

from lib import db
from lib import cli
from lib import ngen
from lib import logger
from pathlib import Path
from configparser import ConfigParser
from multiprocessing import Pool

#############################################################################

def f(args):
    (index, nid, (window, threshold)) = args
    
    log.info('{0} create'.format(nid))

    node = nd.Node(nid)
    speed = node.readings.speed
    classifier = cp.Acceleration(threshold)

    log.info('{0} apply'.format(nid))

    srs = pd.Series()
    for (l, r) in node.range(window):
        left = speed[l]
        if left.isnull().values.any():
            break
        lmean = left.mean()
        
        for i in itertools.count():
            index = r.union(r + i)
            right = speed[index]
            if right.isnull().values.any():
                break
            rmean = right.mean()
            
            if not classifier.classify(window.prediction, lmean, rmean):
                break
            
        srs.set_value(r[0], len(index))

    log.info('{0} finish')
    
    return (nid, srs)

#############################################################################

log = logger.getlogger(True)

cargs = cli.CommandLine(cli.optsfile('prediction')) # /etc/opts/prediction
log.info('configure ' + cargs.args.config)

config = ConfigParser()
config.read(cargs.args.config) # --config

# Establish the database credentials. Passing None uses the
# defaults.
dbinfo = config['database'] if 'database' in config else None
db.EstablishCredentials(**dbinfo)

# log.info('db version: {0}'.format(db.mark()))

#
# Processing
#
log.info('processing')

pth = Path(cargs.args.config)
pth = Path(pth.parent, pth.stem)

pth.mkdir()

with Pool() as pool:
    g = ngen.SequentialGenerator()
    w = win.from_config(config)
    a = float(config['parameters']['acceleration'])
    
    for (nid, df) in pool.imap_unordered(f, g.nodegen(w, a), 1):
        d = df[df > 0]
        stats = [ d.mean(), d.std() ]
        for i in stats:
            log.info('stats {0} {1:0.3f}'.format(nid, i))
        
        p = Path(pth, '{0:03d}'.format(nid))
        df.to_pickle(str(p))
