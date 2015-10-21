import os

import machine as m

from lib import db
from lib import cli
from lib import aggregator as ag
from lib.node import nodegen
from lib.logger import log
from collections import namedtuple
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
    (index, node, (cargs,)) = args
    
    log.info('node: {0}'.format(node))

    machine = machine_[cargs.args.model]
    aggregator = aggregator_[cargs.args.aggregator]
    model = machine(node, cargs, aggregator)

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
db.genop(cargs.args.reporting)

#
# Begin the processing!
#
with Pool() as pool:
    results = pool.starmap(f, nodegen([ cargs ]), 1)
    results = list(filter(lambda x: x.values, results))

log.info('phase 2')

if results:
    header = results[0].keys
    with CSVWriter(header, delimiter=';') as writer:
        if cargs.args.header:
            writer.writeheader()
        for i in results:
            writer.writerows(i.values)
        
log.info('phase 3')
