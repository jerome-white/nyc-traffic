import sys
import time
import pickle
import itertools
import collections as cl

from lib import cli
from pathlib import Path
from lib.logger import log
from urllib.error import URLError
from urllib.request import urlopen
from xml.dom.minidom import parse

Attr = cl.namedtuple('Attr', [ 'name', 'process' ])

def sql_time(value):
    tm = time.strptime(value, '%m/%d/%Y %H:%M:%S')
    return time.strftime("%Y-%m-%d %X", tm)

def sql_location(value):
    swap = ', ;'
    swaplen = len(swap)
    for i in range(swaplen):
        j = (i + 2) % swaplen
        value = value.replace(swap[i], swap[j])

    return 'LineFromText({0:s})'.format(value)

tables = {
    'reading': cl.OrderedDict([
        ('node', Attr('Id', int)),
        ('speed', Attr('Speed', float)),
        ('travel_time', Attr('TravelTime', int)),
        ('as_of', Attr('DataAsOf', sql_time)),
    ]),
    'node': cl.OrderedDict([
        ('id', Attr('Id', int)),
        ('name', Attr('Name', str)),
        ('owner', Attr('Owner', str)),
        ('borough', Attr('Borough', str)),
        ('segment', Attr('Segment', sql_location)),
    ]),
}

cargs = cli.CommandLine(cli.optsfile('storage'))
args = cargs.args
tbl = tables[args.table]

#
# Figure out whether we're creating new data, or adding to existing
# data
#
fname = Path(args.output)
if fname.exists():
    with fname.open(mode='rb') as fp:
        data = pickle.load(fp)
else:
    data = []

#
# Get the remote data...
#
for i in itertools.count(0):
    try:
        doc = parse(urlopen(args.url))
        break
    except (URLError, ConnectionError) as err:
        # log.error(u)
        pass

    if i > args.retries:
        log.critical('Retries exceeded')
        sys.exit(1)
        
    time.sleep(args.timeout)

#
# ... and parse it
#
for node in doc.getElementsByTagName(args.root):
    row = {}
    for (key, value) in tbl.items():
        attr = node.getAttribute(value.name)
        row[key] = value.process(attr)
    data.append(row)

#
# Write the data to a file
#
with fname.open(mode='wb') as fp:
    pickle.dump(data, fp)
