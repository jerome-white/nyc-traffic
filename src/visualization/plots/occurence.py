# import matplotlib
# matplotlib.style.use('ggplot')

from pathlib import Path
from argparse import ArgumentParser
from multiprocessing import Pool

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from lib import logger

def boxplot(**kwargs):
    sns.boxplot(palette='PRGn', whis=np.inf, **kwargs)
    sns.stripplot(jitter=True, size=3, color='.3', linewidth=0, **kwargs)

def factorplot(**kwargs):
    sns.factorplot(aspect=2, **kwargs)

def plot(args):
    (source, target, frequencies, title) = args

    log = logger.getlogger()
    log.info('o: {0} p: {1}'.format(*map(int, source.parts[-2:])))
    
    data = [ pd.read_csv(str(x)) for x in source.glob('*.csv') ]
    df = pd.concat(data, axis=1)

    columns = [ 'Hour', 'Segment', 'Average number of jams' ]
    kwargs = { 'x': columns[0], 'y': columns[-1] }
    
    for freq in map(lambda x: str(x) + 'H', frequencies):
        log.info(freq)
        
        df_ = df.resample(freq).sum()
        grouped = df_.groupby(lambda x: x.hour).mean()
        grouped = grouped.stack(level=0).reset_index()
        grouped.columns = columns

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
            f(**kwargs)
            sns.despine(trim=True)

            fname = f.__name__ + '-' + freq
            dest = target.joinpath(fname).with_suffix('.png')

            plt.title(title)
            plt.gcf().savefig(str(dest))
            plt.close()

def mkargs(source, target, frequencies):
    heading = [ 'Observation', 'Prediction' ]
    
    for observation in source.iterdir():
        for prediction in observation.iterdir():
            subdir = Path(observation.stem, prediction.stem)
                    
            title = zip(heading, map(int, subdir.parts))
            title = ' '.join([ ': '.join(map(str, x)) for x in title ])
                    
            dest = target.joinpath(subdir)
            dest.mkdir(parents=True, exist_ok=True)

            yield (prediction, dest, frequencies, title)

arguments = ArgumentParser()
arguments.add_argument('--data', type=Path)
arguments.add_argument('--output', type=Path)
args = arguments.parse_args()
logger.getlogger(True)

with Pool(cpu_count() // 2, maxtasksperchild=1) as pool:
    for _ in pool.imap_unordered(plot, mkargs(source, target, args.freqs)):
        pass
