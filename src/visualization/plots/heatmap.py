from pathlib import Path
from argparse import ArgumentParser
from multiprocessing import Pool

import numpy as np
import pandas as pd
import seaborn as sns

from lib import logger
from lib.window import Window

def func(args):
    (data, freq) = args

    try:
        window = Window.from_path(data.parent)
    except ValueError:
        return

    log = logger.getlogger()
    log.info('{0} {1}'.format(window, data.stem))

    srs = pd.read_csv(str(data),
                      index_col=0,
                      parse_dates=True,
                      squeeze=True,
                      header=None)
    srs = srs.resample(freq).sum()

    return (window.observation, window.offset, srs.mean())

arguments = ArgumentParser()
arguments.add_argument('--data', type=Path)
arguments.add_argument('--output', type=Path)
# http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
arguments.add_argument('--frequency', default='D')
args = arguments.parse_args()

log = logger.getlogger(True)

columns = [ 'observation', 'offset', 'mean' ]
with Pool() as pool:
    iterable = map(lambda x: (x, args.frequency), args.data.glob('**/*.csv'))
    data = filter(None, pool.imap_unordered(func, iterable))
    df = pd.DataFrame.from_records(data, columns=columns)
groups = df.groupby(by=columns[:-1])
table = groups.mean().unstack()
table.columns = table.columns.droplevel() # http://stackoverflow.com/a/22233719

ax = sns.heatmap(table, annot=True, fmt='.0f')
ax.figure.savefig(str(args.output))
