import os

import machine as m

from multiprocessing import Pool

from lib import cli
from lib import aggregator as ag
from lib.node import nodegen
from lib.logger import log
from lib.csvwriter import CSVWriter 

def f(*args):
    (index, node, cargs) = args
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

with Pool() as pool:
    cargs = cli.CommandLine(cli.optsfile('prediction'))

    results = pool.starmap(f, nodegen(cargs), 1)

log.info('phase 2')

results = list(filter(None, results))
(header, body) = hextract(results)
    
with CSVWriter(header, delimiter=';') as writer:
    if cargs.args.header:
        writer.writeheader()
    for i in body:
        writer.writerows(i)
        
log.info('phase 3')
