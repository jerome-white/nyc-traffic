import sys
import time
import pickle
import logging
import requests
import traceback

import collections as cl
import xml.dom.minidom as dom
import xml.etree.ElementTree as et

from lib import cli
from pathlib import Path
from lib.logger import log
from tempfile import NamedTemporaryFile
from requests.exceptions import RequestException

Attr = cl.namedtuple('Attr', [ 'name', 'process' ])

for i in [ 'requests', 'urllib3' ]:
    logging.getLogger(i).setLevel(logging.CRITICAL)

def handle_error(doc):
    with NamedTemporaryFile(mode='w', delete=False) as fp:
        fp.write(doc)
        return fp.name

class GetRemoteXML:
    def __get_data(self, url, retries, timeout):
        elist = []
        for _ in range(retries):
            try:
                r = requests.get(url)
                r.raise_for_status()
            except (RequestException, ConnectionError) as err:
                elist.append(type(err).__name__)
                time.sleep(timeout)
                continue

            return r.text

        log.error(elist)
        raise AttributeError('Retries exceeded')
        
    def __init__(self, url, retries, timeout, reading, node):
        self.doc = self.__get_data(url, retries, timeout)
        self.data = []
        self.time_fmt = ''
        self.parse_function = None
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
        assert(self.parse_function and self.doc)
        
        try:
            return self.parse_function(self.doc)
        except Exception as err:
            fname = handle_error(self.doc)
            msg = '{0} (see {1})'.format(err, fname)
            raise AttributeError(msg)
    
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
            ('name', Attr('LinkName', str)),
            ('segment', Attr('linkPoints', self.sql_location)),
        ]
        super().__init__(url, retries, timeout, reading, node)
        self.time_fmt = '%m/%d/%Y %H:%M:%S'
        self.parse_function = dom.parseString
        
        # self.tables['nyc_node'].update({
        #     'owner': Attr('Owner', str),
        #     'borough': Attr('Borough', str),
        # })
            
    def sql_location(self, value):
        v = []
        for i in value.split(' '):
            try:
                # corrections for erroneous lat/long's: non-numeric,
                # non-paired, out-of-range
                (lat, lon) = [ float(x) for x in i.split(',') ]
                if 40 < lat < 41 and -75 < lon < -72:
                    v.append(' '.join(map(str, (lat, lon))))
            except ValueError:
                continue
        fmt = ','.join(v)

        return super().sql_location(fmt)
    
    def parse(self, table, root='Speed'):
        xml = super().parse(table, root)
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
        self.parse_function = et.fromstring
        
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
        xml = super().parse(table, root)
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
                        try:
                            row[key] = value.process(info)
                        except ValueError:
                            break

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
except AssertionError:
    (*_, tb) = sys.exc_info()
    (*_, tb_info) = map(list, traceback.extract_tb(tb))
    
    if data.doc:
        fname = handle_error(data.doc)
        tb_info.append(fname)
            
    log.critical(' '.join(map(str, tb_info)))
