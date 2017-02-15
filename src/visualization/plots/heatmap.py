import operator as op
from pathlib import Path
from argparse import ArgumentParser
from multiprocessing import Pool

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
arguments.add_argument('--save-data', type=Path)
arguments.add_argument('--from-data', type=Path)
arguments.add_argument('--vmax', type=float)
arguments.add_argument('--vmin', type=float)
# http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
arguments.add_argument('--frequency', default='D')
args = arguments.parse_args()

log = logger.getlogger(True)

columns = [ 'observation', 'offset', 'mean' ]
if args.from_data:
    df = pd.read_csv(str(args.from_data), index_col=0)
else:
    with Pool() as pool:
        glob = args.data.glob('**/*.csv')
        iterable = map(lambda x: (x, args.frequency), glob)
        data = filter(None, pool.imap_unordered(func, iterable))
        df = pd.DataFrame.from_records(data, columns=columns)
    if args.save_data:
        df.to_csv(str(args.save_data))

groups = df.groupby(by=columns[:-1])
table = groups.mean().unstack()
table.columns = table.columns.droplevel() # http://stackoverflow.com/a/22233719

kwargs = { x: op.attrgetter(x)(args) for x in ('vmin', 'vmax') }
ax = sns.heatmap(table, annot=True, fmt='.0f', **kwargs)
ax.figure.savefig(str(args.output))
