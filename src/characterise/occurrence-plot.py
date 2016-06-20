# import matplotlib
# matplotlib.style.use('ggplot')

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

def mkargs(source, target, frequencies):
    heading = [ 'Observation', 'Prediction' ]
    
    for observation in source.iterdir():
        if observation.is_dir():
            for prediction in observation.iterdir():
                if prediction.is_dir():
                    subdir = Path(observation.stem, prediction.stem)
                    
                    title = zip(heading, map(int, subdir))
                    title = ' '.join([ ': '.join(map(str, x)) for x in title ])
                    
                    dest = target.joinpath(subdir)
                    dest.mkdir(parents=True, exist_ok=True)

                    yield (prediction, dest, frequencies, title)

def boxplot(**kwargs):
    sns.boxplot(palette="PRGn", whis=np.inf, **kwargs)
    sns.stripplot(jitter=True, size=3, color='.3', linewidth=0, **kwargs)

def factorplot(**kwargs):
    sns.factorplot(kind='bar', **kwargs)

def plot(source, target, frequencies, title):
    logger.getlogger().info(str(source))
    
    data = [ pd.read_pickle(str(x)) for x in source.glob('*.pkl') ]
    df = pd.concat(data, axis=1)

    columns = [ 'Hour', 'Segment', 'Average number of jams' ]
    kwargs = { 'x': columns[0], 'y': columns[-1] }
    
    for freq in map(lambda x: str(x) + 'H', frequencies):
        df = df.resample(freq).sum()
    
        grouped = df.groupby(lambda x: x.hour).mean()
        grouped = grouped.stack(level=0).reset_index()
        grouped.columns = [ 'Hour', 'Segment', 'Average number of jams' ]

        # grouped.replace(to_replace={grouped.columns[0]: {
        #     0: '00:00-03:59',
        #     4: '04:00-07:59',
        #     8: '08:00-11:59',
        #     12: '12:00-15:59',
        #     16: '16:00-19:59',
        #     20: '20:00-23:59',
        # }}, inplace=True)

        kwargs['data'] = grouped
        for f in [ boxplot, factorplot ]:
            f(args)
            sns.despine(trim=True)

            fname = f.__name__ + '-' + freq
            dest = target.joinpath(fname).with_suffix('.png')

            plt.title(title)
            plt.gcf().savefig(str(dest))
            plt.close()

args = cli.CommandLine(cli.optsfile('characterisation-plot')).args
source = Path(args.source)
target = Path(args.target)

with Pool(cpu_count() // 2, maxtasksperchild=1) as pool:
    pool.starmap(plot, mkargs(source, target, args.freqs))
