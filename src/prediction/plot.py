import operator as op
from pathlib import Path
from argparse import ArgumentParser
from collections import OrderedDict
from configparser import ConfigParser

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from lib import logger
from lib.utils import mkplot_
from lib.window import window_from_config

metrics_ = OrderedDict({
    # 'f1_score': 'F$_{1}$',
    'matthews_corrcoef': 'MCC',
})

columns_ = [
    # 'segment',
    'neighbors',
    'prediction',
    'observation'
]

def mkframes(args, metrics):
    log = logger.getlogger()
    
    usecols = metrics + [ 'frequency' ]
    usewins = [ 'prediction', 'observation' ]
    
    for i in args.results_directory.iterdir():
        ini = i.joinpath('ini')
        if not ini.is_file():
            continue

        config = ConfigParser()
        config.read(str(ini))
        window = window_from_config(config)
        neighbors = int(config['neighbors']['depth'])

        conditions = [ neighbors <= args.max_neighbors ]
        for j in usewins:
            (a, b) = [ getattr(x, j) for x in (args, window) ]
            conditions.append((not a) or (b in a))

        if all(conditions):
            log.info('{0} {1} {2}'.format(i.name, neighbors, window))
            for j in i.joinpath('results').glob('*.csv'):
                df = pd.read_csv(str(j), usecols=usecols)

                (threshold, ) = df.frequency.unique()
                if threshold > args.threshold:
                    continue
                
                df['segment'] = int(j.stem)
                df['neighbors'] = neighbors
                df['prediction'] = window.prediction
                df['observation'] = window.observation
            
                yield df

arguments = ArgumentParser()
arguments.add_argument('--max-neighbors', default=np.inf, type=int)
arguments.add_argument('--neighbors-inclusive', action='store_true')
arguments.add_argument('--results-directory', type=Path)
arguments.add_argument('--observation', type=int, action='append')
arguments.add_argument('--prediction', type=int, action='append')
arguments.add_argument('--threshold', default=np.inf, type=float)
arguments.add_argument('--output', default='.')
args = arguments.parse_args()

if args.results_directory is None or not args.results_directory.exists():
    exit()

for (i, j) in metrics_.items():
    frames = mkframes(args, [ i ])
    df = pd.concat(frames, ignore_index=True, copy=False)
    if args.neighbors_inclusive:
        neighbor_limit = df['neighbors'].max()
        targets = df.segment[df['neighbors'] == neighbor_limit].unique()
        df = df[df.segment.isin(targets)]
    # df.to_pickle('frame.pkl')

    group_columns = filter(lambda x: df[x].unique().size > 0, columns_)
    groups = df.groupby(list(group_columns))

    g = sns.factorplot(x='neighbors',
                       y=i,
                       col='prediction',
                       row='observation',
                       kind='bar',
                       data=df,
    )
    g.set_axis_labels('Neighbors', j)
    g.set(ylim=(0,1))

    path = Path(args.output, i).with_suffix('.png')
    plt.gcf().savefig(str(path))
    plt.close()
