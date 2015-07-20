import os
import machine

from multiprocessing import Pool

from lib import cli
from lib.node import nodegen
from lib.logger import log
from lib.csvwriter import CSVWriter 

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

with Pool() as pool:
    cargs = cli.CommandLine(cli.optsfile('prediction'))

    results = pool.starmap(f, nodegen(cargs), 1)
    results = list(filter(None, results))
    (header, body) = hextract(results)
    
    with CSVWriter(header, delimiter=';') as writer:
        if cargs.args.header:
            writer.writeheader()
        for i in body:
            writer.writerows(i)
