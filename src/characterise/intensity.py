import pickle
import itertools

import pandas as pd
import lib.node as nd
import lib.window as win
import lib.cpoint as cp
import rollingtools as rt

from lib import db
from lib import cli
from lib import ngen
from lib import logger
from pathlib import Path
from configparser import ConfigParser
from multiprocessing import Pool

#############################################################################

def jam_duration(left, right, data, prediction, classify):
    l = data[left]
    if l.isnull().values.any():
        raise ValueError()
    
    lmean = l.mean()
    
    for i in itertools.count():
        index = right.union(right + i)
        r = data[index]
        if r.isnull().values.any():
            break
        
        rmean = r.mean()
            
        if not classify(prediction, lmean, rmean):
            break

    size = len(index) - 1
    if size < len(right):
        raise ValueError()

    return size

def f(args):
    (index, nid, (window, threshold)) = args
    
    log.info('{0} create'.format(nid))

    node = nd.Node(nid)
    classifier = cp.Acceleration(threshold)

    log.info('{0} apply'.format(nid))

    df = pd.Series()
    for (l, r) in node.range(window):
        try:
            duration = jam_duration(l, r, node.readings.speed,
                                    window.prediction, classifier.classify)
            df.set_value(r[0], duration)
        except ValueError:
            continue

    log.info('{0} finish')
    
    return rt.NodeData(nid, df)

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
    
    for result in pool.imap_unordered(f, g.nodegen(w, a), 1):
        d = result.data[result.data > 0]
        stats = [ d.mean(), d.std() ]
        for i in stats:
            log.info('stats {0} {1:0.3f}'.format(result.node, i))
        
        p = Path(pth, '{0:03d}'.format(result.node))
        result.data.to_pickle(str(p))
