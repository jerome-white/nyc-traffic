import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from lib import cli
from lib import logger
from pathlib import Path

def pltvar(data, labels, stem):
    (xlabel, ylabel) = labels

    kwargs = { 'x': xlabel, 'y': 'deviation', 'data': df }
    sns.boxplot(palette="PRGn", whis=np.inf, **kwargs)
    sns.stripplot(jitter=True, size=3, color='.3', linewidth=0, **kwargs)
    
    ax = plt.gca()
    ax.set_xlabel(xlabel.title() + ' window (minutes)')
    ax.set_ylabel(ylabel.title() + ' window std. dev. (jams/day)')

    fname = '-'.join([ 'variance', xlabel, stem ])
    dest = source.joinpath(fname).with_suffix('.png')
    plt.savefig(str(dest))
    plt.close()

args = cli.CommandLine(cli.optsfile('characterisation-plot')).args
source = Path(args.source)
log = logger.getlogger(True)

for i in source.glob('*.pkl'):
    log.info(str(i))
    
    data = pd.read_pickle(str(i))
    
    log.info('visualize: mean')

    df = data.mean(axis=1).unstack('observation')
    df = df.ix[df.index.sort_values(ascending=False)]
    sns.heatmap(df, annot=True, fmt='.0f')
    ax = plt.gca()
    ax.set_xlabel('Adjacent windows (minutes)')
    ax.set_ylabel('Prediction window (minutes)')
    dest = source.joinpath('frequency-' + i.stem).with_suffix('.png')
    plt.savefig(str(dest))
    plt.close()

    log.info('visualize: variance')    

    df = data.std(axis=1)
    df.name = 'deviation'
    df = df.reset_index()
    df.rename(columns={ 'observation': 'adjacent' }, inplace=True)

    pltvar(df, df.columns[:-1], i.stem)
    pltvar(df, df.columns[1::-1], i.stem)
