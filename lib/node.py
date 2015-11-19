import numpy as np
import pandas as pd
import datetime as dt
import collections as coll
import scipy.constants as constant

from lib import db
from tempfile import NamedTemporaryFile
from lib.logger import log
from statsmodels.tsa import stattools as st

def getnodes(connection):
    sql = 'SELECT id FROM operational ORDER BY id ASC'
    
    with db.DatabaseCursor(connection) as cursor:
        cursor.execute(sql)
        for row in cursor:
            yield row['id']
            
def nodegen(args=None):
    k = tuple(args) if type(args) == list else args
    
    with db.DatabaseConnection() as conn:
        for (i, j) in enumerate(getnodes(conn)):
            yield (i, j, k)

def nacount(data, col='speed'):
    return data[col].isnull().sum()

def complete(data, col='speed'):
    return data.size > 0 and nacount(data, col) == 0

def get_neighbors(nid, connection, spatial=True):
    '''
    Get the geospatial neighbors of a node.
    spatial: True: use MySQL spatial functions (5.6+)
             False: use minimum bounding rectangles
    '''
    
    fmt = coll.defaultdict(str)
    fmt['nid'] = nid
    if spatial:
        st_fun = 'ST_DISTANCE(target.segment, source.segment)'
        fmt['order'] = 'ORDER BY ' + st_fun
        fmt['geo'] = 'ST_'
    else:
        fmt['geo'] = 'MBR'
        
    sql = [ 'SELECT target.id AS nid',
            'FROM operational source, operational target',
            'WHERE {1}INTERSECTS(source.segment, target.segment)',
            'AND source.id = {0} AND target.id <> {0} {2}',
            ]
    sql = db.process(sql, [ fmt[x] for x in ('nid', 'geo', 'order') ])
    
    with db.DatabaseCursor(connection) as cursor:
        cursor.execute(sql)
        return frozenset([ row['nid'] for row in cursor ])

class Node:
    def __init__(self, nid, connection=None, freq='T'):
        self.nid = nid
        self.freq = freq

        close = not connection
        if close:
            connection = db.DatabaseConnection().resource
            
        self.name = self.__get_name(connection)            
        self.readings = self.__get_readings(connection)
        # self.neighbors = get_neighbors(self.nid, connection)

        if close:
            connection.close()

    def __repr__(self):
        return str(self.nid)
    
    def __str__(self):
        return repr(self) + ': ' + self.name

    def __eq__(self, other):
        return self.nid == other.nid

    def __hash__(self):
        return self.nid
    
    def __get_name(self, connection):
        sql = [ 'SELECT name',
                'FROM operational',
                'WHERE id = {0}',
                ]
        sql = db.process(sql, [ self.nid ])
        
        with db.DatabaseCursor(connection) as cursor:
            cursor.execute(sql)
            if cursor.rowcount != 1:
                err = '{0} does not exist!'.format(self.nid)
                raise AttributeError(err)
            row = cursor.fetchone()

        return row['name']
        
    def __get_readings(self, connection):
        sql = [ 'SELECT as_of, speed, travel_time / {1} AS travel',
                'FROM reading',
                'WHERE node = {0}',
                'ORDER BY as_of ASC',
                ]
        sql = db.process(sql, [ self.nid, constant.minute ])
        
        data = pd.read_sql_query(sql, con=connection, index_col='as_of')
        data.columns = [ 'speed', 'travel' ]
        
        return data.resample(self.freq) if self.freq else data

    def range(self, window, bound=True):
        idx = self.readings.index
        
        for (i, _) in enumerate(idx):
            j = i + window.observation
            k = j + window.prediction
            l = k + window.target
            
            if bound and l > idx.size:
                raise StopIteration
            
            yield (idx[i:j], idx[k:l])
    
    def align(self, other, interpolate=False):
        data = self.readings.reindex(other.readings.index)
        if interpolate:
            # XXX think about limiting this!
            data = data.interpolate().fillna(method='backfill')

        self.readings = data

    def partition(self, pct):
        assert(0 <= pct <= 1)

        line = round(len(self.readings) * pct)

        return (self.readings.ix[:line], self.readings.ix[line:])

    def stationary(self, sig=0.05):
        (adf, pvalue, _, _, crit, *_) = st.adfuller(self.readings)

        return pvalue < sig and adf < max(crit.values())
    
    def purge(self, thresh):
        data = self.readings
        condition = np.abs(data - data.mean()) <= thresh * data.std()

        return self.readings[condition]
