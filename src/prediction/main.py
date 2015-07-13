import os
import machine

from csvwriter import CSVWriter 
from multiprocessing import Pool

from lib import cli
from lib.node import nodegen
from lib.logger import log

def f(*args):
    (index, node, cargs) = args
    log.info('node: {0}'.format(node))
    
    if cargs.args.model == 'classification':
        model = machine.Classifier(node, cargs)
    elif cargs.args.model == 'estimation':
        model = machine.Estimator(node, cargs)
    else:
        raise AttributeError('Unrecognized machine type')

    results = model.predict(model.classify())
    if not index:
        results.append(model.header())
    
    return results

def hextract(results):
    header = None
    for entry in results:
        if isinstance(entry[-1], list):
            header = entry.pop()
            break
        
    return (header, results)

with Pool() as pool:
    cargs = cli.CommandLine(cli.optsfile('main'))
    
    results = list(filter(None, pool.starmap(f, nodegen(cargs))))
    (header, body) = hextract(results)
    
    with CSVWriter(header, delimiter=';') as writer:
        if cargs.args.header:
            writer.writeheader()
        for i in body:
            writer.writerows(i)
