import tempfile
import itertools
from pathlib import Path
from argparse import ArgumentParser
from collections import namedtuple
from multiprocessing import Pool

from lib import logger
from lib import cpoint
from lib.window import Window
from lib.ledger import Ledger
from lib.segment import Segment

Entry = namedtuple('Entry', 'segment, observation, offset')

#############################################################################

def count(stop=None, start=1, inclusive=True):
    if inclusive:
        stop += 1
    yield from range(start, stop)

def func(args):
    (opts, entry) = args

    log = logger.getlogger()
    log.info(' '.join(map(str, entry)))

    segment = Segment(entry.segment)
    window = Window(entry.observation, entry.offset)
    classifier = cpoint.Selector(opts.classifier)(opts.alpha)
    series = segment.roller(window).apply(classifier.classify, args=(window, ))

    path = Path(opts.output, window.topath(), str(segment))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix('.csv').open('w') as fp:
        series.dropna().to_csv(fp)

    return entry

def enumerator(records, args):
    data = sorted(args.data.glob('*.csv'))

    for segment in itertools.islice(data, args.node, None, args.total_nodes):
        for observation in count(args.max_observations):
            for offset in count(args.max_offset):
                entry = Entry(segment, observation, offset)
                if entry not in records:
                    yield (args, entry)

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

if args.ledger:
    ledger = args.ledger
else:
    ledger = Path(tempfile.mkdtemp(suffix='-ledger'))
    log.info('Initialised ledger directory: {0}'.format(str(ledger)))

log.info('|> {0}/{1}'.format(args.node, args.total_nodes))
with Ledger(ledger, args.node, Entry) as records:
    with Pool() as pool:
        for i in pool.imap_unordered(func, enumerator(records, args)):
            records.record(i)
log.info('|< {0}/{1}'.format(args.node, args.total_nodes))
