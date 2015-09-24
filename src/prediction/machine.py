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

from lib import data
from lib.db import DatabaseConnection
from tempfile import NamedTemporaryFile
from lib.logger import log
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

ClsProbs = namedtuple('ClsProbs', [ 'valid', 'probabilities' ])
ClassifierFactory = namedtuple('ClassifierFactory', [ 'construct', 'kwargs' ])

class Machine:
    def __init__(self, nid, cli, aggregator):
        self.nid = nid
        self.cli = cli
        self.net = None
        self.args = self.cli.args
        self.aggregator = aggregator

        self.nhandler = {
            'simple': cl.Cluster,
            'var': cl.VARCluster,
            'hybrid': cl.HybridCluster,
            }
        
        self.metrics_ = []
        self.header_ = [
            'implementation',
            'shape',
            'node',
            'network',
            'kfold',
            ]
        self.classifiers_ = {}
        self.probs = ClsProbs(False, None)
        
    def header(self):
        return self.header_ + self.cli.options() + self.metrics()
    
    def classify(self):
        observations = []
        window = nd.Window(self.args.window_obs,
                           self.args.window_pred,
                           self.args.window_trgt)
        #
        # create the network of nodes (the source and its neighbors)
        #
        cluster = self.nhandler[self.args.nselect]
        self.network = nt.Network(self.nid, self.args.neighbors, cluster)

        depth = self.network.depth()
        if depth != self.args.neighbors:
            msg = 'Depth not fully explored: {0} < {1}'
            raise ValueError(msg.format(depth, self.args.neighbors))
        
        root = self.network.node
        assert(root.nid == self.nid)
        nodes = []
        for i in self.network:
            nodes.append(i.node)
            if i.node != root:
                i.align_and_shift(root)
                
        #
        # Build the observation matrix by looping over the root nodes
        # time periods
        #
        for (i, j) in root.range(window):
            try:
                label = self._label(root, i, j)
                features = self._features(nodes, i)
                observations.append(features + label)
            except ValueError:
                continue
            
        log.debug('observations: {0}'.format(len(observations)))
                
        return observations
        
    def predict(self, observations):
        predictions = []

        for i in self.args.classifier:
            try:
                (ptr, kwargs) = self.classifiers_[i]
                clf = ptr(**kwargs)
            except KeyError:
                continue

            for (j, k) in enumerate(data.kfold(observations, self.args.folds)):
                msg = '{0}: prediction {1} of {2}'
                log.info(msg.format(ptr.__name__, j, self.args.folds))

                #
                # train and fit the model
                #
                (x_train, x_test, y_train, y_test) = k
                try:
                    clf.fit(x_train, y_train)
                    pred = clf.predict(x_test)
                except (AttributeError, ValueError) as error:
                    with NamedTemporaryFile(mode='wb', delete=False) as fp:
                        pickle.dump(observations, fp)
                        msg = '{0}: {1} {2}'.format(i, error, fp.name)
                        log.error(msg)
                    continue
                
                self.set_probabilities(clf, x_test)
                
                #
                # add accounting information to result row
                #
                lst = [
                    ptr.__name__,  # implementation
                    x_train.shape, # shape
                    self.nid,      # node
                    repr(self.network),  # network
                    j,             # (k)fold
                    ]
                assert(len(lst) == len(self.header_))
                d = dict(zip(self.header_, lst))
                d.update(self.args.__dict__)
                
                #
                # run all of the desired metrics and add them to the
                # result row
                #
                with warnings.catch_warnings():
                    warnings.filterwarnings('error')
                    for f in self.metrics_:
                        try:
                            d[f.__name__] = f(y_test, pred)
                        except (UndefinedMetricWarning, ValueError) as error:
                            log.warning(error)
                    
                predictions.append(d)

        return predictions

    def __tostr(self, vals):
        return [ i.__name__ for i in vals ]
    
    def metrics(self):
        return self.__tostr(self.metrics_)

    def classifiers(self):
        return self.__tostr(self.classifiers_)

    def _features(self, nodes, left):
        raise NotImplementedError()
    
    def _label(self, node, left, right):
        raise NotImplementedError()

    def set_probabilities(self, clf, x):
        raise NotImplementedError()
    
class Classifier(Machine):
    def __init__(self, nid, cli, aggregator=ag.simple):
        super().__init__(nid, cli, aggregator)
        
        self.metrics_ = [
            self.confusion_matrix,
            self.roc,
            sklearn.metrics.accuracy_score,
            sklearn.metrics.f1_score,
            sklearn.metrics.matthews_corrcoef,
        ]

        self.classifiers_ = {
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
            assert(len(values) == self.args.window_obs)
            distilled = self.aggregator(values)
            features.extend(distilled)

        return features

    def _label(self, node, left, right):
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
        assert(int(duration) == self.args.window_pred)

        label = cp.changed(duration, l, r, self.args.threshold)
         
        return [ int(label) ]

class Estimator(Machine):
    def __init__(self, nid, cli, aggregator=ag.simple):
        super().__init__(nid, cli, aggregator)
        
        self.metrics_ = [
            sklearn.metrics.explained_variance_score,
            sklearn.metrics.mean_absolute_error,
            sklearn.metrics.mean_squared_error,
        ]

        classifiers_ = {
            'svm': SVR,
            'tree': DecisionTreeRegressor,
            'bayes': GaussianNB,
            'forest': RandomForestRegressor,
        }
