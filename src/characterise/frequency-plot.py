import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from lib import cli
from lib import logger
from pathlib import Path

def pltvar(data, x, labels, stem):
    (xlabel, ylabel) = labels

    kwargs = { 'x': x, 'y': 'deviation', 'data': df }
    sns.boxplot(palette="PRGn", whis=np.inf, **kwargs)
    sns.stripplot(jitter=True, size=3, color='.3', linewidth=0, **kwargs)
    
    ax = plt.gca()
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel + ' window std. dev. (jams/day)')

    fname = '-'.join([ 'variance', x, stem ])
    dest = source.joinpath(fname).with_suffix('.png')
    plt.savefig(str(dest))
    plt.close()

args = cli.CommandLine(cli.optsfile('characterisation-plot')).args
source = Path(args.source)
xlabel = 'Adjacent windows (minutes)'

log = logger.getlogger(True)

for i in source.glob('*.pkl'):
    log.info(str(i))
    
    data = pd.read_pickle(str(i))
    
    log.info('visualize: mean')

    df = data.mean(axis=1).unstack('observation')
    df = df.ix[df.index.sort_values(ascending=False)]
    sns.heatmap(df, annot=True, fmt='.0f')
    ax = plt.gca()
    ax.set_xlabel(xlabel)
    ax.set_ylabel('Prediction window (minutes)')
    dest = source.joinpath('frequency-' + i.stem).with_suffix('.png')
    plt.savefig(str(dest))
    plt.close()

    log.info('visualize: variance')    

    df = data.std(axis=1)
    df.name = 'deviation'
    df = df.reset_index()
    
    kwargs = {
        'data': df,
        'x': 'prediction',
        'labels': [ xlabel, 'Observation' ],
        'stem': i.stem,
        }
    pltvar(**kwargs)
    
    kwargs['x'] = 'observation'
    kwargs['labels'][1] = 'Prediction'
    pltvar(**kwargs)
