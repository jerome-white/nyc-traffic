import machine

from db import DatabaseConnection
from cli import CommandLine
from node import getnodes
from logger import log
from os.path import expanduser
from csvwriter import CSVWriter 
from multiprocessing import Pool

def stargen(cli):
    with DatabaseConnection() as conn:
        for (i, j) in enumerate(getnodes(conn)):
            yield (i, j, cli)

def f(*args):
    (index, node, cli) = args
    log.info('node: {0}'.format(node))
    
    if cli.args.model == 'classification':
        model = machine.Classifier(node, cli)
    elif cli.args.model == 'estimation':
        model = machine.Estimator(node, cli)
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
    cli = CommandLine(expanduser('~/.trafficrc/opts.main'))
    
    results = list(filter(None, pool.starmap(f, stargen(cli))))
    (header, body) = hextract(results)
    
    with CSVWriter(header, delimiter=';') as writer:
        if cli.args.header:
            writer.writeheader()
        for i in body:
            writer.writerows(i)
