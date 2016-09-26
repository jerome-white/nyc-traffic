import operator as op
from pathlib import Path
from argparse import ArgumentParser
from collections import OrderedDict
from configparser import ConfigParser

import numpy as np
import pandas as pd

from lib import logger
from lib.utils import mkplot_
from lib.window import window_from_config

def mkframes(args, metrics):
    log = logger.getlogger()
    usecols = list(metrics) + [ 'frequency' ]
    
    for i in args.results_directory.iterdir():
        ini = i.joinpath('ini')
        if not ini.is_file():
            continue

        config = ConfigParser()
        config.read(str(ini))
        window = window_from_config(config)
        neighbors = int(config['neighbors']['depth'])

        R = op.eq if args.neighbors_inclusive else op.le
        conditions = [
            R(neighbors, args.max_neighbors),
            not args.prediction or window.prediction in args.prediction,
            not args.observation or window.observation in args.observation,
        ]
        # for j in ('prediction', 'observation'):
        #     (a, b) = getattr(x, j) for x in (args, window)
        #     conditions.append(not a or b in a)
            
        if all(conditions):
            for j in i.joinpath('results').glob('*.csv'):
                df = pd.read_csv(str(j), usecols=usecols)

                threshold = df.frequency.unique()
                assert(len(threshold) == 1)
                if threshold[0] > args.threshold:
                    continue
            
                log.info(i)

                df['segment'] = int(j.stem)
                df['neighbors'] = neighbors
                df['prediction'] = window.prediction
                df['observation'] = window.observation
            
                yield df

arguments = ArgumentParser()
arguments.add_argument('--max-neighbors', default=0, type=int)
arguments.add_argument('--neighbors-inclusive', action='store_true')
arguments.add_argument('--results-directory', type=Path)
arguments.add_argument('--observation', type=int, action='append')
arguments.add_argument('--prediction', type=int, action='append')
arguments.add_argument('--threshold', default=np.inf, type=float)
args = arguments.parse_args()

if args.results_directory is None or not args.results_directory.exists():
    exit()

metrics = OrderedDict({
    # 'f1_score': 'F$_{1}$',
    'matthews_corrcoef': 'MCC',
})

frames = mkframes(args, metrics.keys())
df = pd.concat(frames, ignore_index=True, copy=False)
df.to_pickle('frame.pkl')

# groups = df.groupby(['segment'])
# g = groups[list(metrics.keys())]
# (means, errors) = [ f() for f in (g.mean, g.sem) ]

# # yticks = np.linspace(0, 1, 11)
# fig = means.plot(kind='bar', yerr=errors, ylim=(0,1)) # , yticks=yticks)
# mkplot_(fig, 'plot.png')
