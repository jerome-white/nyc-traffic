from lib import db
from lib import ngen
from lib import logger
from lib.node import Node
from pathlib import Path
from multiprocessing import Pool

def func(args):
    log = logger.getlogger()
    log.info(args)
    
    try:
        n = Node(args, freq=None)
        path = Path('/', 'Volumes', 'Untitled', '{0:03d}'.format(args))
        path = path.with_suffix('.pkl')
        n.readings.to_pickle(str(path))
    except AttributeError:
        pass
    
    return args

with Pool() as pool:
    log = logger.getlogger(True)
    iterable = ngen.SequentialGenerator('node')
    for _ in pool.imap_unordered(func, iterable.getnodes()):
        pass
