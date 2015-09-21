import os

import machine as m

from multiprocessing import Pool

from lib import db
from lib import cli
from lib import aggregator as ag
from lib.node import nodegen
from lib.logger import log
from lib.csvwriter import CSVWriter 

def f(*args):
    (index, node, (cargs,)) = args
    
    log.info('node: {0}'.format(node))

    machine_ = {
        'classification': m.Classifier,
        'estimation': m.Estimator,
        }
    machine = machine_[cargs.args.model]
    
    aggregator_ = {
        'simple': ag.simple,
        'change': ag.change,
        'average': ag.average,
        'difference': ag.difference,
        }
    aggregator = aggregator_[cargs.args.aggregator]

    model = machine(node, cargs, aggregator)
    results = model.predict(model.classify())
    if index == 0:
        results.append(model.header())
    
    return results

def hextract(results):
    header = None
    for entry in results:
        if isinstance(entry[-1], list):
            header = entry.pop()
            break
        
    return (header, results)

log.info('phase 1')
log.info('db version {0}'.format(db.mark()))

cargs = cli.CommandLine(cli.optsfile('prediction'))
with DatabaseConnection() as connection:
    sql = ('CREATE OR REPLACE VIEW operational AS ' +
           'SELECT n.id, n.name, n.segment ' +
           'FROM node AS n ' +
           'JOIN quality AS q ON n.id = q.node ' +
           'WHERE q.frequency <= {0}')
    sql = sql.format(cargs.args.reporting)
    with DatabaseCursor(connection) as cursor:
        sql.execute(sql)

with Pool() as pool:
    results = pool.starmap(f, nodegen([ cargs ]), 1)

log.info('phase 2')

results = list(filter(None, results))
(header, body) = hextract(results)
    
with CSVWriter(header, delimiter=';') as writer:
    if cargs.args.header:
        writer.writeheader()
    for i in body:
        writer.writerows(i)
        
log.info('phase 3')
