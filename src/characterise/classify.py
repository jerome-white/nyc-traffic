import operator as op
import itertools
from pathlib import Path
from tempfile import mkdtemp
from argparse import ArgumentParser
from collections import namedtuple
from multiprocessing import Pool

from lib import logger
from lib import cpoint
from lib.window import Window
from lib.ledger import Ledger
from lib.segment import Segment

Entry = namedtuple('Entry', 'segment, observation, offset')
Args = namedtuple('Args', 'entry, classifier, output')

#############################################################################

def count(stop=None, start=1, inclusive=True):
    rel = op.gt if inclusive else op.ge
    for i in itertools.count(start):
        if stop and rel(i, stop):
            break
        yield i

def func(args):
    log = logger.getlogger()
    
    window = Window(args.entry.observation, args.entry.offset)
    classifier = cpoint.Selector(args.classifier)
    segment = Segment(args.entry.segment)
    log.info(segment)

    kwargs = { 'window': window }
    series = segment.roller(window).apply(classifier.classify, kwargs=kwargs)

    win = [ '{0:02d}'.format(x) for x in (window.observation, window.offset) ]
    path = Path(args.output, *win, str(segment))
    path.mkdir(parents=True, exists_ok=True)
    with path.with_suffix('.csv').open('w') as fp:
        series.to_csv(fp)

    return args.entry

def enumerator(records, args):
    data = sorted(args.data.glob('*.csv'))

    for segment in itertools.islice(data, args.node, None, args.total_nodes):
        for observation in count(args.max_observations):
            for offset in count(args.max_offset):
                entry = Entry(segment, observation, offset)
                if entry not in records:
                    yield Args(entry, args.classifier, args.output)

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

if not args.ledger:
    tmp = mkdtemp(suffix='-ledger')
    log.info('Initialised ledger directory: '.format(tmp))
    args.ledger = Path(tmp)

log.info('|> {0}/{1}'.format(args.node, args.total_nodes))
with Ledger(args.ledger, args.node, Entry) as records:
    with Pool() as pool:
        for i in pool.imap_unordered(func, enumerator(records, args)):
            records.record(*i)
log.info('|< {0}/{1}'.format(args.node, args.total_nodes))
