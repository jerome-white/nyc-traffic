import pickle
import warnings

import numpy as np
import lib.cluster as cl
import lib.network as nt

from lib import logger
from lib import configtools
from tempfile import NamedTemporaryFile
from lib.window import Window
from collections import namedtuple
from sklearn.metrics.base import UndefinedMetricWarning
from sklearn.cross_validation import StratifiedShuffleSplit

ClsProbs = namedtuple('ClsProbs', [ 'valid', 'probabilities' ])

class Machine:
    def __init__(self, nid, config, aggregator):
        self.nid = nid
        self.config = config
        self.aggregator = aggregator
        self.network = None
        self.jam_classifier = None
        self.log = logger.getlogger()

        self.nhandler = {
            'simple': cl.Cluster,
            'var': cl.VARCluster,
            'hybrid': cl.HybridCluster,
            }
        
        self._metrics = []
        self._header = [
            'implementation',
            'shape',
            'node',
            'network',
            'kfold',
            ]
        self._machines = {}
        self.probs = ClsProbs(False, None)
        
    def classify(self):
        w = [ 'observation', 'prediction', 'target' ]
        window = Window(*[ int(self.config['window'][x]) for x in w ])
        
        #
        # Create the network of nodes (the source and its neighbors)
        #
        self.log.info('{0} + create network'.format(self.nid))
        n = self.config['neighbors']
        cluster = self.nhandler[n['selection']]
        self.network = nt.Network(self.nid, int(n['depth']), cluster)
        self.log.info('{0} - create network'.format(self.nid))
        
        depth = self.network.depth()
        if depth != int(n['depth']):
            msg = 'Depth not fully explored: {0} < {1}'
            raise ValueError(msg.format(depth, n['depth']))
        
        root = self.network.node
        assert(root.nid == self.nid)
        
        #
        # Clean up the nodes: All nodes are shifted, interpolated, and
        # time-aligned with the root. Missing values at the root are
        # maintained to determine which windows are valid to process.
        #
        s = root.readings.speed
        missing = s[s.isnull()].index
        
        for i in self.network:
            n = i.node
            n.readings.shift(i.lag)
            n.align(root, True)
                
        #
        # Build the observation matrix by looping over the root nodes
        # time periods
        #
        self.log.info('{0} + observe'.format(self.nid))
        observations = []
        for i in root.range(window):
            whole = [ missing.intersection(x).size == 0 for x in i ]
            if all(whole):
                (left, right) = i
                mid = left[-len(right):]
                assert(len(mid) == len(right))
                
                labels = self._labels(root, mid, right)
                features = self._features(self.network.nodes(), left)
                
                observations.append(features + labels)
        self.log.info('{0} - observe {1}'.format(self.nid, len(observations)))
                
        return observations

    #
    # Splits the data into training and test sets. Returns stratified
    # portions as a generator
    #
    def stratify(self, observations, folds, testing=0.2, no_labels=1):
        assert(0 < testing < 1)

        data = np.asfarray(observations)
        assert(np.isfinite(data).all())

        labels = data[:,-no_labels:].ravel()
        s = StratifiedShuffleSplit(labels, n_iter=folds, test_size=testing)
        if not self.legal_stratification(s):
            raise StopIteration
        features = data[:,:-no_labels]
        
        for (train, test) in s:
            x = (features[train], features[test], labels[train], labels[test])
            yield x

    #
    # Cycle through requested learning methods. Returns an instance
    # of the method and its name as a generator
    #
    def machinate(self, methods):
        wanted = set(methods.split(','))
        for i in wanted.intersection(self._machines.keys()):
            factory = self._machines[i]
            instance = factory.construct(**factory.kwargs)
            
            yield (factory.construct.__name__, instance)
            
    def predict(self, observations):
        predictions = []
        network = repr(self.network)
        args = self.config['machine']

        #
        # build k stratifications
        #
        stratifications = self.stratify(observations, int(args['folds']))

        #
        # for each machine, train/predict using each stratification
        #
        self.log.info('{0} + prediction'.format(self.nid))
        for (i, j) in enumerate(stratifications):
            (x_train, x_test, y_train, y_test) = j

            for (name, clf) in self.machinate(args['method']):
                #
                # train and fit the model
                #
                try:
                    clf.fit(x_train, y_train)
                    pred = clf.predict(x_test)
                except (AttributeError, ValueError) as error:
                    with NamedTemporaryFile(mode='wb', delete=False) as fp:
                        pickle.dump(observations, fp)
                        msg = [ name, error, fp.name ]
                        self.log.error('{0}: {1} {2}'.format(*msg))
                    continue
                
                self.set_probabilities(clf, x_test)
                
                #
                # add accounting information to result row
                #
                lst = [
                    name,          # implementation
                    x_train.shape, # shape
                    self.nid,      # node
                    network,       # network
                    i,             # (k)fold
                    ]
                assert(len(lst) == len(self._header))
                
                d = dict(zip(self._header, lst))
                d.update(configtools.ordered(self.config))
                
                #
                # run all of the desired metrics and add them to the
                # result row
                #
                with warnings.catch_warnings():
                    warnings.filterwarnings('error')
                    for f in self._metrics:
                        try:
                            d[f.__name__] = f(y_test, pred)
                        except (UndefinedMetricWarning, ValueError) as error:
                            self.log.warning(error)
                    
                predictions.append(d)
        self.log.info('{0} prediction {1}'.format(self.nid, len(predictions)))
        
        return predictions

    def header(self):
        h = configtools.ordered(self.config)
        return self._header + list(h.keys()) + self.metrics()

    def metrics(self):
        return [ x.__name__ for x in self._metrics ]

    # Abstract methods

    def _features(self, nodes, left):
        raise NotImplementedError()
    
    def _labels(self, node, left, right):
        raise NotImplementedError()

    def set_probabilities(self, clf, x):
        raise NotImplementedError()

    def legal_stratification(self, strat):
        raise NotImplementedError()
