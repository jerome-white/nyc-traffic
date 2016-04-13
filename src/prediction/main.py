import sys
from estimator import Estimator
from classifier import Classifier

from lib import db
from lib import cli
from csv import DictWriter
from lib import logger
from lib import aggregator as ag
from lib.node import nodegen
from collections import namedtuple
from configparser import ConfigParser
from multiprocessing import Pool

Results = namedtuple('Results', [ 'keys', 'values', ])
class ResultsWriter:
    def __init__(self, header):
        self.header = header
        self.writer = None

    def write(self, results):
        if not results.values:
            return
        
        if not self.writer:
            self.writer = DictWriter(sys.stdout, results.keys, delimiter=';')
            if self.header:
                self.writer.writeheader()
                
        self.writer.writerows(results.values)

#
# Mappings between configuration options and learning
# interfaces. Dictionary keys should have a corresponding key in the
# .ini file!
#
machine_ = {
    'classification': Classifier,
    'estimation': Estimator,
}

aggregator_ = {
    'simple': ag.simple,
    'change': ag.change,
    'average': ag.average,
    'difference': ag.difference,
}

#
# Run the prediction!
#
def run(args):
    (index, node, (config,)) = args

    log = logger.getlogger()
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

#
# Setup
#

log = logger.getlogger(True)
log.info('phase 1')
log.info('db version: {0}'.format(db.mark()))

cargs = cli.CommandLine(cli.optsfile('prediction')) # /etc/opts/prediction
config = ConfigParser()
config.read(cargs.args.config) # --config

params = config['parameters']
writer = ResultsWriter(config['output'].getboolean('print-header'))

# Establish the database credentials. Passing None uses the
# defaults.
dbinfo = config['database'] if 'database' in config else None
db.EstablishCredentials(**dbinfo)

#
# Processing
#
log.info('phase 2')

if 'node' in params:
    args = (0, int(params['node']), config)
    writer.write(run(args))
else:
    with Pool() as pool:
        for i in pool.imap_unordered(run, nodegen(config), 1):
            writer.write(i)

#
# Tear down
#
log.info('phase 3')

