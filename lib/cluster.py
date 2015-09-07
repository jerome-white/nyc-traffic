import numpy as np
import pandas as pd
import scipy.constants as constant
import statsmodels.tsa.vector_ar.var_model as vm

from numpy.linalg import LinAlgError

from lib import db
from lib import node as nd
from lib.logger import log

threshold_ = 0.01

class Cluster:
    def __init__(self, nid, connection=None, freq='T'):
        self.nid = nid

        close = not connection
        if close:
            connection = db.DatabaseConnection().resource
        
        self.neighbors = nd.get_neighbors(self.nid, connection)
        self.readings = self.__get_readings(self.neighbors, connection, freq)
            
        if close:
            connection.close()

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

    def lag(self, nid, threshold=threshold_):
        sql = ('SELECT ROUND(AVG(travel_time) / {0}) AS lag ' +
               'FROM reading ' +
               'WHERE node = {1}')
        sql = sql.format(constant.minute, nid)
        
        with db.DatabaseConnection() as connection:
            with db.DatabaseCursor(connection) as cursor:
                cursor.execute(sql)
                row = cursor.fetchone()

                return row['lag']
    
class VARCluster(Cluster):
    def __init__(self, nid, connection=None, freq='T', maxlags=20):
        super().__init__(nid, connection, freq)

        endog = self.readings.dropna()
        if endog.empty:
            raise AttributeError('Endogenous variable is empty')
        try:
            model = vm.VAR(endog=endog)
            fit = model.fit(maxlags=maxlags)
            self.irf = fit.irf(maxlags)
        except (LinAlgError, ValueError) as err:
            raise AttributeError(err)

    def lag(self, nid, threshold=threshold_):
        idxs = [ 0, self.readings.columns.tolist().index(str(nid)) ]

        #
        # Get the maximum impact in both directions of the shock:
        #   vals[0]: nid -> self.nid
        #   vals[1]: self.nid -> nid
        #
        vals = [ self.irf.irfs[:,x,y] for (x, y) in zip(idxs, idxs[::-1]) ]
        (incoming, outgoing) = [ np.amax(x) for x in vals ]
        
        difference = abs(incoming - outgoing) / ((incoming + outgoing) / 2)
        if incoming < outgoing or difference < threshold:
            raise ValueError('Invalid: {0} {1}'.format(incoming, outgoing))
        
        return np.argmax(vals[0])

class HybridCluster(VARCluster):
    def __init__(self, nid, connection=None, freq='T', maxlags=20):
        super().__init__(nid, connection, freq, maxlags)

    def lag(self, nid, threshold=threshold_):
        super().lag(nid, threshold)

        # Getting to this point means VARCluster's lag didn't raise an
        # exception. "Linearization" then returns Cluster's lag
        
        return super(VARCluster, self).lag(nid, threshold)
