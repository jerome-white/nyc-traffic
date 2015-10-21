import time
import pickle
import collections as cl

from lib import cli
from pathlib import Path
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
    'reading': cl.OrderedDict({
        'node': Attr('Id', int),
        'speed': Attr('Speed', float),
        'travel_time': Attr('TravelTime', int),
        'as_of': Attr('DataAsOf', sql_time),
    }),
    'node': cl.OrderedDict({
        'id': Attr('Id', int),
        'name': Attr('Name', str),
        'owner': Attr('Owner', str),
        'borough': Attr('Borough', str),
        'segment': Attr('Segment', sql_location),
    }),
}

cargs = cli.CommandLine(cli.optsfile('storage'))
args = cargs.args
assert(args.table in tables)

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
# Get the remote data and parse it
#
doc = parse(urlopen(args.url))
for node in doc.getElementsByTagName('Speed'):
    row = {}
    for (key, value) in tables[args.table].items():
        attr = node.getAttribute(value.name)
        row[key] = value.process(attr)
    data.append(row)

#
# Write the data to a file
#
with fname.open(mode='wb') as fp:
    pickle.dump(data, fp)
