import multiprocessing as mp
from argparse import ArgumentParser

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from lib import logger
from lib.window import Window
from lib.cpoint import Acceleration

def func(args):
    (gravity, offsets, limit) = args

    log = logger.getlogger()
    acc = Acceleration(gravity)
    cutoffs = np.full((offsets, limit), np.nan)

    for o in range(offsets):
        log.debug('{0} {1}'.format(gravity, o))

        win = Window(offset=o+1)
        observations = [ np.nan ] * len(win)

        for start in range(limit):
            observations[0] = start
            for stop in range(limit):
                observations[-1] = stop
                if not acc.classify(observations, win):
                    cutoffs[o, start] = stop
                    break

    df = pd.DataFrame(cutoffs.T, columns=range(1, offsets + 1))

    return (gravity, df)

def each(args):
    for i in range(4, 4 * 10, 4):
        yield (i / 1e4, args.offsets, args.speed_limit)

arguments = ArgumentParser()
arguments.add_argument('--offsets', type=int)
arguments.add_argument('--speed-limit', type=int)
arguments.add_argument('--workers', type=int, default=mp.cpu_count())
args = arguments.parse_args()

log = logger.getlogger(True)

with mp.Pool(args.workers) as pool:
    for (i, j) in pool.imap_unordered(func, each(args)):
        log.info(i)

        title = str(i)
        j.plot.line(grid=True, title=title)
        plt.savefig(title + '.png')
