import sys
import machine

from lib import db
from lib import cli
from lib import aggregator
from csv import DictWriter
from lib.node import nodegen
from lib.logger import log
from collections import namedtuple
from configparser import ConfigParser
from multiprocessing import Pool

Results = namedtuple('Results', [ 'keys', 'values', ])

machine_ = {
    'classification': machine.Classifier,
    'estimation': machine.Estimator,
}

aggregator_ = {
    'simple': aggregator.simple,
    'change': aggregator.change,
    'average': aggregator.average,
    'difference': aggregator.difference,
}
    
def f(args):
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
log.info('phase 2')

with Pool() as pool:
    writer = None
    for results in pool.imap_unordered(f, nodegen([ config ]), 1):
        if results.values:
            if not writer:
                writer = DictWriter(sys.stdout, results.keys, delimiter=';')
                if config['output'].getboolean('print-header'):
                    writer.writeheader()
            writer.writerows(results.values)
