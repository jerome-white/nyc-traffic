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

def mkargs(top_level, freq):
    for i in top_level.iterdir():
        if i.is_dir():
            for j in i.iterdir():
                if j.is_dir():
                    yield (j, freq)

def accumulate(path, freq):
    (observation, prediction) = [ int(x) for x in path.parts[-2:] ]

    data = [ pd.read_pickle(str(x)) for x in path.glob('*.pkl') ]
    df = pd.concat(data, axis=1)
    df = df.resample(freq).sum().mean()

    return (observation, prediction, df.mean())

log = logger.getlogger(True)

args = cli.CommandLine(cli.optsfile('characterisation-plot')).args
top_level = Path(args.source)
target = Path(args.target)
target.mkdir(parents=True, exist_ok=True)

frequencies = args.freqs if args.freqs else [ 'D' ] # XXX defaults?

for freq in args.freqs:
    log.info('collect {0}'.format(freq))

    df = pd.DataFrame()
    df.index.name = 'Adjacent windows (minutes)' # observation
    df.columns.name = 'Prediction window (minutes)' # prediction
    with Pool(cpu_count() // 2, maxtasksperchild=1) as pool:
        for i in pool.imap_unordered(accumulate, mkargs(top_level)):
            log.info('{0} {1}'.format(*i[:2]))
            df.set_value(*i)
    df = df.ix[df.index.sort_values(),
               df.columns.sort_values(ascending=False)].T

    log.info('visualize')

    sns.heatmap(df, annot=True, fmt='.0f')

    fname = 'frequency-' + freq
    dest = target.joinpath(fname).with_suffix('.png'))
    plt.savefig(str(dest))
