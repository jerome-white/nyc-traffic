import tempfile
import itertools
from pathlib import Path
from argparse import ArgumentParser
from collections import namedtuple
from multiprocessing import Pool

import numpy as np

from lib import logger
from lib import cpoint
from lib.window import Window
from lib.ledger import Ledger
from lib.segment import Segment

Entry = namedtuple('Entry', 'segment, observation, offset')
Options = namedtuple('Options', 'segment, ledger, args')

#############################################################################

def dealer(args):
    counter = lambda x: map(int, np.linspace(1, x, num=x))

    with Ledger(args.ledger, args.node, Entry) as ledger:
        for observation in counter(args.max_observations):
            for offset in counter(args.max_offset):
                entry = Entry(args.node, observation, offset)
                if entry not in ledger:
                    yield Window(observation, offset)
                    ledger.record(entry)

def func(opts):
    (segment, args) = opts

    log = logger.getlogger()

    segment = Segment(segment)
    classifier = cpoint.Selector(args.classifier)(args.alpha)

    for window in dealer(args):
        log.info('s: {0}, w: {1}'.format(segment, window))

        roller = segment.roller(window)
        series = roller.apply(classifier.classify, args=(window, ))

        path = Path(args.output, window.topath(), str(segment))
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.with_suffix('.csv').open('w') as fp:
            series.dropna().to_csv(fp)

############################################################################

arguments = ArgumentParser()
arguments.add_argument('--data', type=Path)
arguments.add_argument('--ledger', type=Path)
arguments.add_argument('--output', type=Path)
arguments.add_argument('--alpha', type=float, default=-0.002)
arguments.add_argument('--classifier', default='acceleration')
arguments.add_argument('--max-observations', type=int)
arguments.add_argument('--max-offset', type=int)
arguments.add_argument('--node', type=int, default=0)
arguments.add_argument('--total-nodes', type=int, default=1)
args = arguments.parse_args()

log = logger.getlogger(True)

log.info('|> {0}/{1}'.format(args.node, args.total_nodes))
with Pool() as pool:
    data = sorted(args.data.glob('*.csv'))
    segments = itertools.islice(data, args.node, None, args.total_nodes)
    for i in pool.imap_unordered(func, map(lambda x: (x, args), segments)):
        log.info('s: {0} finished'.format(segment))
log.info('|< {0}/{1}'.format(args.node, args.total_nodes))
