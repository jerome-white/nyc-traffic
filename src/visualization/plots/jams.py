import itertools as it
import multiprocessing as mp
from pathlib import Path
from argparse import ArgumentParser

import numpy as np

from lib import logger
from lib.window import Window
from lib.cpoint import Acceleration

def func(args):
    (gravity, offset, limit) = args

    log = logger.getlogger()
    acc = Acceleration(gravity)
    win = Window(offset=offset)

    cutoffs = [ np.nan ] * limit
    observations = [ np.nan ] * len(win)

    for start in range(limit):
        observations[0] = start
        for stop in range(limit):
            observations[-1] = stop
            if not acc.classify(observations, win):
                cutoffs[start] = stop
                break

    return (gravity, offset, cutoffs)

def each(args):
    for i in range(4, 4 * 10, 4):
        gravity = i / 1e4
        for j in range(args.offsets):
            yield (gravity, j + 1, args.speed_limit)

arguments = ArgumentParser()
arguments.add_argument('--offsets', type=int)
arguments.add_argument('--speed-limit', type=int)
arguments.add_argument('--workers', type=int, default=mp.cpu_count())
args = arguments.parse_args()

log = logger.getlogger(True)

with mp.Pool(args.workers) as pool:
    for i in pool.imap_unordered(func, each(args)):
        print(i)
