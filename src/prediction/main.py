import os

import machine as m

from lib import db
from lib import cli
from lib import aggregator as ag
from lib.node import nodegen
from lib.logger import log
from collections import namedtuple
from configparser import ConfigParser
from lib.csvwriter import CSVWriter
from multiprocessing import Pool

Results = namedtuple('Results', [ 'keys', 'values', ])

machine_ = {
    'classification': m.Classifier,
    'estimation': m.Estimator,
}

aggregator_ = {
    'simple': ag.simple,
    'change': ag.change,
    'average': ag.average,
    'difference': ag.difference,
}
    
def f(*args):
    (index, node, (config,)) = args
    
    log.info('node: {0}'.format(node))

    opts = config['machine']
    machine = machine_[opts['model']]
    aggregator = aggregator_[opts['feature-transform']]
    model = machine(node, config, aggregator)

    keys = model.header()
    values = []
    try:
        values = model.predict(model.classify())
    except ValueError as v:
        log.error(v)

    return Results(keys, values)

log.info('phase 1')
log.info('db version {0}'.format(db.mark()))

cargs = cli.CommandLine(cli.optsfile('prediction'))
config = ConfigParser()
config.read(cargs.args.config)

db.genop(int(config['parameters']['intra-reporting']))

#
# Begin the processing!
#
with Pool() as pool:
    results = pool.starmap(f, nodegen([ config ]), 1)
    results = [ x for x in results if x.values ]
    assert(results)

log.info('phase 2')

header = results[0].keys
with CSVWriter(header, delimiter=';') as writer:
    if config['output'].getboolean(['print-header']):
        writer.writeheader()
    for i in results:
        writer.writerows(i.values)
            
log.info('phase 3')
