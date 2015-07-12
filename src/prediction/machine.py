import pickle
import warnings
import sklearn.metrics

import numpy as np
import datetime as dt

from tempfile import NamedTemporaryFile
from collections import deque
from sklearn.svm import SVC
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics.metrics import UndefinedMetricWarning

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
            # nodes = nd.neighbors(source, self.args.neighbors, conn)
            delay = lambda x, y: 5
            nodes = nd.neighbors(source, self.args.neighbors, conn, delay)
    
            msg = ', '.join(map(lambda x: ':'.join(map(repr, x)), nodes))
            log.debug('{0}: {1}'.format(source.nid, msg))

        aggregator = ag.PctChangeAggregator(len(nodes), win.observations)
        
        for (i, j) in source.range(window):
            try:
                label = self._label(source, left, right)
                features = self._features(nodes, left, aggregator)
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
                    ptr.__name__,
                    x_train.shape,
                    self.nid,
                    j,
                ]
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
            df = n.node.readings
            if n.node.nid == self.nid:
                lag = 0
            else:
                lag = df.travel.ix[left].mean()
            values = df.speed.shift(lag).ix[left]
            assert(len(values) == self.args.window_obs)
            distilled = aggregator.aggregate(values)
            
            bad = np.count_nonzero(~np.isfinite(distilled))
            if bad > 0:
                msg = '{0}|{1}-{2} {3}|{4}'
                msg = msg.format(n.node, left, right, values, distilled)
                log.info(msg)
                
            features.extend(distilled)

        return features

    def _label(self, node, left, right):
        (l, r) = [ node.readings.speed.ix[i] for i in (left, right) ]
         
        if not (nd.complete(l) and nd.complete(r)):
            msg = 'Incomplete measurement interval: {0}: {1}({3})-{2}({4})'
            err = msg.format(self.nid, left[0], right[0],
                             left.size, right.size)
            raise ValueError(err)
        
        (x, y) = [ i.values.mean() for i in (l, r) ]
        label = cp.changed(self.args.window_pred, x, y, self.args.threshold)
         
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
