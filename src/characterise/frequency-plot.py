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

def accumulate(args):
    (path, freq) = args
    (observation, prediction) = [ int(x) for x in path.parts[-2:] ]

    logger.getlogger().info('o: {0} p: {1}'.format(observation, prediction))
    
    data = [ pd.read_pickle(str(x)) for x in path.glob('*.pkl') ]
    df = pd.concat(data, axis=1)
    df = df.resample(freq).sum().mean()

    return (observation, prediction, df.mean())

args = cli.CommandLine(cli.optsfile('characterisation-plot')).args
top_level = Path(args.source)
target = Path(args.target)
target.mkdir(parents=True, exist_ok=True)

freqs = args.freqs if args.freqs else [ 'D' ] # XXX defaults?

log = logger.getlogger(True)

for fq in freqs:
    log.info('collect {0}'.format(fq))

    df = pd.DataFrame()
    df.index.name = 'Adjacent windows (minutes)' # observation
    df.columns.name = 'Prediction window (minutes)' # prediction
    with Pool(cpu_count() // 2, maxtasksperchild=1) as pool:
        for i in pool.imap_unordered(accumulate, mkargs(top_level, fq)):
            df.set_value(*i)
    df = df.ix[df.index.sort_values(),
               df.columns.sort_values(ascending=False)].T

    log.info('visualize')

    sns.heatmap(df, annot=True, fmt='.0f')

    fname = 'frequency-' + fq
    dest = target.joinpath(fname).with_suffix('.png')
    plt.savefig(str(dest))
