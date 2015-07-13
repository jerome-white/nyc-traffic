import numpy as np
import pandas as pd
import statsmodels.tsa.vector_ar.var_model as vm

from numpy.linalg import LinAlgError

from lib import db
from lib.logger import log

class Cluster:
    def __init__(self, nid, freq='T'):
        self.nid = nid
        with db.DatabaseConnection() as conn:
            self.neighbors = self.__get_neighbors(conn)
            self.readings = self.__get_readings(self.neighbors, conn, freq)

    def addlag(self, lag, inclusive=False, delimiter='-'):
        cols = list(self.neighbors)
        if inclusive:
            cols += [ self.nid ]

        for i in map(str, cols):
            column = delimiter.join([ i, str(lag) ])
            self.readings[column] = self.readings[i].shift(lag)
        
    def __repr__(self):
        return str(self.nid)

    def __str__(self):
        return '{0:03d}'.format(self.nid)

    def __where_clause(self, neighbors, splt=3):
        a = ' node = '.join(map(str, [''] + neighbors)).split()
        b = [ a[x:x + splt] for x in range(0, len(a), splt) ]
        
        return ' or '.join([ ' '.join(x) for x in b ])
        
    def __get_readings(self, neighbors, connection, freq):
        nodes = list(neighbors) + [ self.nid ]
        sql = ('SELECT as_of, node, speed ' +
               'FROM reading ' +
               'WHERE {0}')
        sql = sql.format(self.__where_clause(nodes))
        
        data = pd.read_sql_query(sql, con=connection)
        data.reset_index(inplace=True)
        data = data.pivot(index='as_of', columns='node', values='speed')

        # Make this node id the first column
        cols = data.columns
        i = cols.tolist().index(self.nid)
        cols = np.roll(cols, -i)
        data = data.ix[:,cols]

        data.columns = data.columns.astype(str)
        
        return data.resample(freq)

    def __get_neighbors(self, connection):
        sql = ('SELECT target.id AS id ' +
               'FROM node source, node target ' +
               'WHERE INTERSECTS(source.segment, target.segment) ' +
               'AND source.id = {0} AND target.id <> {0}')
        sql = sql.format(self.nid)

        with db.DatabaseCursor(connection) as cursor:
            cursor.execute(sql)

            return frozenset([ row['id'] for row in cursor ])

class VARCluster(Cluster):
    def __init__(self, nid, maxlags=20):
        super().__init__(nid)

        endog = self.readings.dropna()
        if endog.empty:
            raise AttributeError('Endogenous variable is empty')
        try:
            model = vm.VAR(endog=endog)
            fit = model.fit(maxlags=maxlags)
            self.irf = fit.irf(maxlags)
        except (LinAlgError, ValueError) as err:
            raise AttributeError(err)

    def lag(self, nid, threshold=0.01):
        idxs = [ 0, self.readings.columns.tolist().index(str(nid)) ]
        vals = [ self.irf.irfs[:,x,y] for (x, y) in zip(idxs, idxs[::-1]) ]
        (incoming, outgoing) = [ np.amax(x) for x in vals ]
        
        difference = abs(incoming - outgoing) / ((incoming + outgoing) / 2)
        if incoming < outgoing or difference < threshold:
            raise ValueError('Invalid: {0} {1}'.format(incoming, outgoing))

        return np.argmax(vals[0])
