import pickle
import warnings
import sklearn.metrics

import numpy as np
import datetime as dt
import lib.node as nd
import lib.cpoint as cp
import lib.cluster as cl
import lib.network as nt
import lib.aggregator as ag
import scipy.constants as constant

from lib import configtools
from lib.db import DatabaseConnection
from tempfile import NamedTemporaryFile
from lib.logger import log
from lib.window import Window
from collections import namedtuple
from sklearn.svm import SVC
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from sklearn.tree import DecisionTreeClassifier
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics.base import UndefinedMetricWarning
from sklearn.cross_validation import StratifiedShuffleSplit

ClsProbs = namedtuple('ClsProbs', [ 'valid', 'probabilities' ])
ClassifierFactory = namedtuple('ClassifierFactory', [ 'construct', 'kwargs' ])

class Machine:
    def __init__(self, nid, config, aggregator):
        self.nid = nid
        self.config = config
        self.aggregator = aggregator
        self.network = None
        self.jam_classifier = None

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
        n = self.config['neighbors']
        cluster = self.nhandler[n['selection']]
        self.network = nt.Network(self.nid, int(n['depth']), cluster)

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
            
        log.debug('observations: {0}'.format(len(observations)))
                
        return observations

    def stratify(self, observations, folds, testing=0.2, no_labels=1):
        assert(0 < testing < 1)

        data = np.asfarray(observations)
        assert(np.isfinite(data).all())

        features = data[:,:-no_labels]
        labels = data[:,-no_labels:].ravel()
        
        s = StratifiedShuffleSplit(labels, n_iter=folds, test_size=testing)

        if self.jam_classifier:
            if s.classes.size != self.jam_classifier.categories:
                raise StopIteration

        for (train, test) in s:
            x = (features[train], features[test], labels[train], labels[test])
            yield x
        
    def predict(self, observations):
        predictions = []
        network = repr(self.network)
        args = self.config['machine']

        #
        # subset of machines to use
        #
        wanted = set(args['method'].split(','))
        keys = wanted.intersection(self._machines.keys())
        machines = { x: self._machines[x] for x in keys }

        #
        # build k stratifications
        #
        stratifications = self.stratify(observations, int(args['folds']))

        #
        # for each machine, train/predict using each stratification
        #
        for (i, j) in enumerate(stratifications):
            (x_train, x_test, y_train, y_test) = j

            for m in machines.values():
                clf = m.construct(**m.kwargs)

                msg = '{0}: prediction {1} of {2}'
                log.info(msg.format(m.construct.__name__, i, args['folds']))

                #
                # train and fit the model
                #
                try:
                    clf.fit(x_train, y_train)
                    pred = clf.predict(x_test)
                except (AttributeError, ValueError) as error:
                    with NamedTemporaryFile(mode='wb', delete=False) as fp:
                        pickle.dump(observations, fp)
                        msg = '{0}: {1} {2}'.format(m, error, fp.name)
                        log.error(msg)
                    continue
                
                self.set_probabilities(clf, x_test)
                
                #
                # add accounting information to result row
                #
                lst = [
                    m.construct.__name__,  # implementation
                    x_train.shape,         # shape
                    self.nid,              # node
                    network,               # network
                    i,                     # (k)fold
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
                            log.warning(error)
                    
                predictions.append(d)

        return predictions

    def __tostr(self, vals):
        return [ x.__name__ for x in vals ]

    def header(self):
        h = configtools.ordered(self.config)
        return self._header + list(h.keys()) + self.metrics()

    def metrics(self):
        return self.__tostr(self._metrics)

    # def classifiers(self):
    #     return self.__tostr(self._machines)

    def _features(self, nodes, left):
        raise NotImplementedError()
    
    def _labels(self, node, left, right):
        raise NotImplementedError()

    def set_probabilities(self, clf, x):
        raise NotImplementedError()
    
class Classifier(Machine):
    def __init__(self, nid, config, aggregator=ag.simple, jam=cp.Acceleration):
        super().__init__(nid, config, aggregator)
        
        self._metrics = [
            self.confusion_matrix,
            self.roc,
            sklearn.metrics.accuracy_score,
            sklearn.metrics.f1_score,
            sklearn.metrics.matthews_corrcoef,
        ]

        self._machines = {
            'svm': ClassifierFactory(SVC, {
                'cache_size': 2000, # 2GB
                # 'probability': True,
            }),
            'tree': ClassifierFactory(DecisionTreeClassifier, {}),
            'bayes': ClassifierFactory(GaussianNB, {}),
            'dummy': ClassifierFactory(DummyClassifier, {}),
            'boost': ClassifierFactory(GradientBoostingClassifier, {}),
            'forest': ClassifierFactory(RandomForestClassifier, {}),
        }

        threshold = float(self.config['parameters']['acceleration'])
        self.jam_classifier = jam(threshold)

    def set_probabilities(self, clf, x):
        try:
            self.probs = ClsProbs(True, clf.predict_proba(x))
        except AttributeError as err:
            # this generally happens if a classifier doesn't natively
            # support probability estimates (such as SVMs; set
            # 'probability' in this case)
            log.warning(err)

    def roc(self, y_true, y_pred):
        if self.probs.valid:
            p = self.probs.probabilities
            try:
                (fpr, tpr, _) = sklearn.metrics.roc_curve(y_true, p[:,1])
                return sklearn.metrics.auc(fpr, tpr)
            except IndexError:
                err = 'Invalid probability matrix: {0}'
                err = err.format(p.shape)
                raise ValueError(err)
                                                        
    def confusion_matrix(self, y_true, y_pred):
        cm = sklearn.metrics.confusion_matrix(y_true, y_pred)
        cv = cm.flatten()
        if len(cv) != 4:
            err = 'Invalid confusion matrix: {0}'.format(cm.shape)
            raise ValueError(err)

        return ','.join(map(str, cv))

    def _features(self, nodes, left):
        features = []
        
        for i in nodes:
            values = i.readings.speed.ix[left]
            assert(len(values) == int(self.config['window']['observation']))
            distilled = self.aggregator(values)
            features.extend(distilled)

        return features

    def _labels(self, node, left, right):
        means = []
        for i in (left, right):
            series = node.readings.speed.ix[i]
            nans = series.isnull().values.sum()
            if nans > 0:
                msg = '{0}: Incomplete interval: {1}-{2} {3} of {4}'
                err = msg.format(node, i[0], i[-1], nans, len(i))
                raise ValueError(err)
            means.append(series.mean())
        (l, r) = means

        gap = right[0] - left[-1]
        duration = (gap.total_seconds() / constant.minute) - 1
        assert(int(duration) == int(self.config['window']['prediction']))
        
        label = self.jam_classifier.classify(duration, l, r)
        
        return [ int(label) ]

class Estimator(Machine):
    def __init__(self, nid, config, aggregator=ag.simple):
        super().__init__(nid, config, aggregator)
        
        self._metrics = [
            sklearn.metrics.explained_variance_score,
            sklearn.metrics.mean_absolute_error,
            sklearn.metrics.mean_squared_error,
        ]

        self._machines = {
            'svm': SVR,
            'tree': DecisionTreeRegressor,
            'bayes': GaussianNB,
            'forest': RandomForestRegressor,
        }
