from pathlib import Path
from argparse import ArgumentParser
from multiprocessing import Pool

import numpy as np
import pandas as pd
import seaborn as sns

from lib import logger
from lib.window import Window

def func(args):
    log = logger.getlogger()
    log.debug(args)

    window = Window.from_path(args.parent)

    srs = pd.read_csv(str(args), index_col=0, parse_dates=True, squeeze=True,
                      header=None)
    srs = srs.resample('H').sum()
    
    return (window.observation, window.offset, srs.mean())

arguments = ArgumentParser()
arguments.add_argument('--data', type=Path)
arguments.add_argument('--output', type=Path)
args = arguments.parse_args()

log = logger.getlogger(True)

columns = [ 'observation', 'offset', 'mean' ]
with Pool(maxtasksperchild=1) as pool:
    data = pool.imap_unordered(func, args.data.glob('**/*.csv'))
    df = pd.DataFrame.from_records(data, columns=columns)

df.sort_values(by=columns, inplace=True)
ax = sns.heatmap(df.pivot(*columns), annot=True, fmt='.0f')
ax.figure.savefig(str(args.output))
