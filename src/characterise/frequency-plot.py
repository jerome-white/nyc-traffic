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

def acc(args):
    (path, freq) = args
    (observation, prediction) = [ int(x) for x in path.parts[-2:] ]

    logger.getlogger().info('o: {0} p: {1}'.format(observation, prediction))
    
    data = [ pd.read_pickle(str(x)) for x in path.glob('*.pkl') ]
    df = pd.concat(data, axis=1)
    df = df.resample(freq).sum().mean()

    return (observation, prediction, df)

args = cli.CommandLine(cli.optsfile('characterisation-plot')).args
top_level = Path(args.source)
target = Path(args.target)
target.mkdir(parents=True, exist_ok=True)

freqs = args.freqs if args.freqs else [ 'D' ] # XXX defaults?

xlabel = 'Adjacent windows (minutes)'
names = [ 'observation', 'prediction' ]
log = logger.getlogger(True)

for fq in freqs:
    log.info('collect {0}'.format(fq))

    with Pool(cpu_count() // 2, maxtasksperchild=1) as pool:
        f = pool.imap_unordered
        d = { tuple(i): j.values for (*i, j) in f(acc, mkargs(top_level, fq)) }
    index = pd.MultiIndex.from_tuples(d.keys(), names=names)
    data = pd.DataFrame(list(d.values()), index=index).sort_index()
        
    log.info('visualize: mean')

    df = data.mean(axis=1).unstack('observation')
    df = df.ix[df.index.sort_values(ascending=False)]
    sns.heatmap(df, annot=True, fmt='.0f')
    ax = plt.gca()
    ax.set_xlabel(xlabel)
    ax.set_ylabel('Prediction window (minutes)')
    fname = 'frequency-' + fq
    dest = target.joinpath(fname).with_suffix('.png')
    plt.savefig(str(dest))
    plt.close()

    log.info('visualize: variance')    

    df = data.std(axis=1)
    df.name = 'deviation'
    df = df.reset_index()
    kwargs = { 'x': 'observation', 'y': 'deviation', 'data': df }
    sns.boxplot(palette="PRGn", whis=np.inf, **kwargs)
    sns.stripplot(jitter=True, size=3, color='.3', linewidth=0, **kwargs)
    ax = plt.gca()
    ax.set_xlabel(xlabel)
    ax.set_ylabel('Prediction window std. dev. (jams/day)')
    fname = 'variance-' + fq
    dest = target.joinpath(fname).with_suffix('.png')
    plt.savefig(str(dest))
    plt.close()
