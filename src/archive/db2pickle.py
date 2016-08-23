import pandas as pd

from lib import db
from lib import ngen
from lib import logger
from pathlib import Path
from argparse import ArgumentParser
from lib.ngen import SequentialGenerator
from collections import namedtuple
from multiprocessing import Pool

Handler = namedtuple('Handler', 'extension, method')

def func(args):
    (node, output, handler) = args
    
    log = logger.getlogger()
    log.info(node)

    sql = [ 'SELECT as_of, speed, travel_time',
            'FROM reading',
            'WHERE node = {0}',
    ]
    sql = db.process(sql, node)
    with db.DatabaseConnection() as con:
        df = pd.read_sql_query(sql, con=con, index_col='as_of')

    path = Path(output, str(node)).with_suffix('.' + handler.extension)
    archive = getattr(df, handler.method)
    archive(str(path))
    
def enum(args):
    handler = {
        'csv': Handler('csv', 'to_csv'),
        'pickle': Handler('pkl', 'to_pickle'),
    }[args.format]
    gen = SequentialGenerator('node')
    
    yield from map(lambda x: (x, args.output, handler), gen.getnodes())

arguments = ArgumentParser()
arguments.add_argument('--output')
arguments.add_argument('--format')
args = arguments.parse_args()

with Pool() as pool:
    for _ in pool.imap_unordered(func, enum(args)):
        pass
