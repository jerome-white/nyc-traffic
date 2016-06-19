import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from lib import db
from lib import cli
from lib import logger
from pathlib import Path

from multiprocessing import Pool
from multiprocessing import cpu_count

def mkargs(args):
    root = Path(args.plotdir)
    for i in root.iterdir():
        for j in i.iterdir():
            if j.is_dir():
                yield (j, '4H') # args.gfilter)

def plot(path, freq):
    logger.getlogger().info(str(path))
    
    data = [ pd.read_pickle(str(x)) for x in path.glob('*.pkl') ]
    df = pd.concat(data, axis=1)
    df = df.resample(freq).sum()
    
    grouped = df.groupby(lambda x: x.hour).mean()
    grouped = grouped.stack(level=0).reset_index()
    grouped.columns = [ 'Hour', 'Segment', 'Jam average' ]

    grouped.replace(to_replace={grouped.columns[0]: {
        0: '00:00-03:59',
        4: '04:00-07:59',
        8: '08:00-11:59',
        12: '12:00-15:59',
        16: '16:00-19:59',
        20: '20:00-23:59',
        }}, inplace=True)

    args = { 'x': grouped.columns[0],
             'y': grouped.columns[-1],
             'data': grouped }
    sns.boxplot(palette="PRGn", whis=np.inf, **args)
    sns.stripplot(jitter=True, size=3, color='.3', linewidth=0, **args)
    sns.despine(offset=10, trim=True)

    fname = str(path.joinpath('boxplot').with_suffix('.png'))
    plt.gcf().savefig(fname)

args = cli.CommandLine(cli.optsfile('prediction-plot')).args

with Pool(cpu_count() // 2) as pool:
    pool.starmap(plot, mkargs(args))
