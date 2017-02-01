from pathlib import Path
from argparse import ArgumentParser
from multiprocessing import Pool

import numpy as np
import pandas as pd
import seaborn as sns

from lib import logger

def walk(source):
    for i in source.iterdir():
        yield from i.iterdir()

def func(args):
    log = logger.getlogger()
    log.debug(args)

    window = Window.from_path(args)

    f = lambda x: pd.read_csv(str(x), index_col=0, parse_dates)
    df = pd.concat(map(f, source.glob('*.csv')), axis=1)
    df = df.resample('H').sum()
    
    return (window.observation, window.offset, df.mean().mean())

arguments = ArgumentParser()
arguments.add_argument('--data', type=Path)
arguments.add_argument('--output', type=Path)
args = arguments.parse_args()

log = logger.getlogger(True)

columns = [ 'observation', 'offset', 'mean' ]
with Pool() as pool:
    df = pd.DataFrame.from_records(pool.imap_unordered(func, walk(args.data)),
                                   columns=columns)
ax = sns.heatmap(df.pivot(*columns), annot=True, fmt='.0f')
ax.figure.savefig(str(args.output))
