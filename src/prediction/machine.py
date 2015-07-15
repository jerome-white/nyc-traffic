import pickle
import warnings
import sklearn.metrics

import numpy as np
import datetime as dt
import scipy.constants as constant

from tempfile import NamedTemporaryFile
from collections import deque
from sklearn.svm import SVC
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics.base import UndefinedMetricWarning

from lib import data
from lib import node as nd
from lib import cpoint as cp
from lib import aggregator as ag
from lib.db import DatabaseConnection
from lib.logger import log
from lib.csvwriter import CSVWriter

class Machine:
    def __init__(self, nid, cli):
        self.nid = nid
        self.cli = cli
        self.args = self.cli.args
        
        self.metrics_ = []
        self.header_ = [
            'implementation',
            'shape',
            'node',
            'kfold',
            'cluster',
            ]
        self.classifiers_ = {}
        self.probs = None
        
    def header(self):
        return self.header_ + self.cli.options() + self.metrics()

    def _features(self, nodes, left, aggregator):
        return []
    
    def _label(self, node, left, right):
        return []
    
    def classify(self):
        observations = []
        window = nd.Window(self.args.window_obs,
                           self.args.window_pred,
                           self.args.window_trgt)
        
        with DatabaseConnection() as conn: 
            source = nd.Node(self.nid, connection=conn)
            nodes = nd.neighbors(source, self.args.neighbors, conn)

        msg = ', '.join(map(lambda x: ':'.join(map(repr, x)), nodes))
        log.debug('{0}: {1}'.format(source.nid, msg))
        self.cluster = [ (x.node.nid, x.lag) for x in nodes ]

        aggregator = ag.PctChangeAggregator(len(nodes), window.observation)
        
        for (i, j) in source.range(window):
            # log.info('{0}: {1} {2}'.format(self.nid, i[0], j[0]))
            try:
                label = self._label(source, i, j)
                features = self._features(nodes, i, aggregator)
                observations.append(features + label)
            except ValueError as verr:
                # log.error(verr)
                pass
                
        return observations
        
    def predict(self, observations):
        predictions = []

        for i in self.args.classifier:
            if i not in self.classifiers_:
                continue
            ptr = self.classifiers_[i]
    
            # XXX this is hack
            clf = ptr(probability=True) if ptr.__name__ == 'SVC' else ptr()

            for (j, k) in enumerate(data.kfold(observations, self.args.folds)):
                msg = '{0}: prediction {1} of {2}'
                log.info(msg.format(ptr.__name__, j, self.args.folds))
                              
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
            
                lst = [
                    ptr.__name__,  # implementation
                    x_train.shape, # shape
                    self.nid,      # node
                    j,             # (k)fold
                    self.cluster,  # cluster
                ]
                assert(len(lst) == len(self.header))
                d = dict(zip(self.header_, lst))
                d.update(self.args.__dict__)
        
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

    def set_probabilities(self, clf, x):
        return

class Classifier(Machine):
    def __init__(self, nid, cli):
        super().__init__(nid, cli)
        
        self.metrics_ = [
            self.confusion_matrix,
            self.roc,
            sklearn.metrics.matthews_corrcoef,
            sklearn.metrics.accuracy_score,
        ]

        self.classifiers_ = {
            'svm': SVC,
            'bayes': GaussianNB,
            'boost': GradientBoostingClassifier,
            'forest': RandomForestClassifier,
        }

    def set_probabilities(self, clf, x):
        self.probs = clf.predict_proba(x)
        
    def roc(self, y_true, y_pred):
        try:
            prob = self.probs[:,1]
            (fpr, tpr, _) = sklearn.metrics.roc_curve(y_true, prob)
            
            return sklearn.metrics.auc(fpr, tpr)
        except IndexError:
            err = 'Invalid probability matrix: {0}'.format(self.probs.shape)
            raise ValueError(err)

    def confusion_matrix(self, y_true, y_pred):
        cm = sklearn.metrics.confusion_matrix(y_true, y_pred)
        cv = cm.flatten()
        if len(cv) != 4:
            err = 'Invalid confusion matrix: {0}'.format(cm.shape)
            raise ValueError(err)

        return ','.join(map(str, cv))

    def _features(self, nodes, left, aggregator):
        features = []
        
        for n in nodes:
            (df, lag) = (n.node.readings, n.lag)
            # lag = 0 if n.root else df.travel.ix[left].mean()
            if lag > 0:
                df = df.shift(lag)
            values = df.speed.ix[left]
            assert(len(values) == self.args.window_obs)
            
            distilled = aggregator.aggregate(values)
            # bad = np.count_nonzero(~np.isfinite(distilled))
            # if bad > 0:
            #     msg = '{0}|{1}-{2} {3}|{4}'
            #     msg = msg.format(n.node, left, right, values, distilled)
            #     log.info(msg)
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
        duration = (gap.total_seconds() / constant.minute) + 1
        assert(int(duration) == self.args.window_pred)

        label = cp.changed(duration, l, r, self.args.threshold)
         
        return [ int(label) ]

class Estimator(Machine):
    def __init__(self, nid, cli):
        super().__init__(nid, cli)
        
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
