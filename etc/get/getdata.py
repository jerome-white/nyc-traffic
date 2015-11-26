import sys
import time
import pickle
import itertools

import collections as cl
import xml.dom.minidom as dom
import xml.etree.ElementTree as et

from lib import cli
from pathlib import Path
from lib.logger import log
from urllib.error import URLError
from urllib.request import urlopen

Attr = cl.namedtuple('Attr', [ 'name', 'process' ])

class GetRemoteXML:
    def __init__(self, url, retries, timeout, reading, node):
        for i in itertools.count(0):
            try:
                self.doc = urlopen(url)
                break
            except (URLError, ConnectionError) as err:
                log.error(u)
                pass

            if i > retries:
                raise AttributeError('Retries exceeded')
        
            time.sleep(timeout)

        self.data = []
        self.time_fmt = ''
        self.tables = {
            'reading': cl.OrderedDict(reading),
            'node': cl.OrderedDict(node),
        }

    def sql_time(self, value):
        tm = time.strptime(value, self.time_fmt)
        return time.strftime("%Y-%m-%d %X", tm)

    def sql_location(self, value):
        return 'LineFromText({0:s})'.format(value)

    def to_file(self, fname):
        path = Path(fname)
        if path.exists():
            with path.open(mode='rb') as fp:
                existing = pickle.load(fp)
                assert(type(existing) == type(self.data))
                self.data.extend(existing)

        with path.open(mode='wb') as fp:
            pickle.dump(self.data, fp)

    def check(self, fname):
        with open(fname, mode='rb') as fp:
            existing = pickle.load(fp)
            for i in existing:
                print(i)
            
    def parse(self, table, root):
        raise NotImplementedError()

class NYC(GetRemoteXML):
    def __init__(self, url, retries, timeout):
        reading = [
            ('node', Attr('Id', int)),
            ('speed', Attr('Speed', float)),
            ('travel_time', Attr('TravelTime', int)),
            ('as_of', Attr('DataAsOf', self.sql_time)),
        ]
        node = [
            ('id', Attr('Id', int)),
            ('name', Attr('Name', str)),
            ('segment', Attr('Segment', self.sql_location)),
        ]
        super().__init__(url, retries, timeout, reading, node)
        self.time_fmt = '%m/%d/%Y %H:%M:%S'
        
        # self.tables['nyc_node'].update({
        #     'owner': Attr('Owner', str),
        #     'borough': Attr('Borough', str),
        # })
            
    def sql_location(self, value):
        swap = ', ;'
        swaplen = len(swap)
        for i in range(swaplen):
            j = (i + 2) % swaplen
            value = value.replace(swap[i], swap[j])

        return super().sql_location(value)
    
    def parse(self, table, root='Speed'):
        xml = dom.parse(self.doc)
        
        tbl = self.tables[table]
        for node in xml.getElementsByTagName(root):
            row = {}
            for (key, value) in tbl.items():
                attr = node.getAttribute(value.name)
                row[key] = value.process(attr)
            self.data.append(row)

class Massachusetts(GetRemoteXML):
    def __init__(self, url, retries, timeout):
        reading = [
            ('node', Attr('PairID', int)),
            ('speed', Attr('Speed', float)),
            ('travel_time', Attr('TravelTime', float)),
            ('as_of', Attr('LastUpdated', self.sql_time)),
        ]
        node = [
            ('id', Attr('PairID', int)),
            ('name', Attr('Title', str)),
            ('segment', Attr('Routes', self.sql_location)),
        ]
        
        super().__init__(url, retries, timeout, reading, node)
        self.time_fmt = '%b-%d-%Y %H:%M:%S %Z'

        # self.tables['mass_node'].update({
        #     'direction':
        #     'origin':
        #     'destination':
        #     'freeflow':
        #     'onid':
        #     'dnid':
        # })

    def routes(self, node):
        pairs = []
        for r in node.findall('Route'):
            points = [ r.find(x).text for x in ('lat', 'lon') ]
            fmt = ' '.join(map(str, points))
            pairs.append(fmt)

        return ','.join(pairs)

    def xpath(self, values, sep='/'):
        return sep.join(values)
        
    def parse(self, table, root='TRAVELDATA'):
        xml = et.parse(self.doc)
        
        tbl = self.tables[table]

        if table == 'reading':
            handler = tbl['as_of']
            path = self.xpath([ root, handler.name ])
            t = xml.findall(path)
            assert(len(t) == 1)
            tstamp = handler.process(t[0].text)

        path = self.xpath([ root, 'PAIRDATA' ])
        for node in xml.findall(path):
            row = { 'as_of': tstamp } if table == 'reading' else {}
            for (key, value) in tbl.items():
                if key not in row:
                    n = node.find(value.name)
                    info = self.routes(n) if key == 'segment' else n.text
                    if info:
                        row[key] = value.process(info)

            if all([ x in row for x in tbl ]):
                self.data.append(row)

class Ireland(GetRemoteXML):
    def __init__(self, url, retries, timeout):
        super().__init__(url, retries, timeout, None, None)
            
processors = {
    'nyc': NYC,
    'mass': Massachusetts,
    'ie': Ireland,
}
cargs = cli.CommandLine(cli.optsfile('storage'))
args = cargs.args

handler = processors[args.source]
try:
    data = handler(args.url, args.retries, args.timeout)
    data.parse(args.table, args.root)
    data.to_file(args.output)
    # data.check(args.output)
except AttributeError as err:
    log.critical(err)

