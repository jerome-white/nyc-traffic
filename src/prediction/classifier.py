import machine
import sklearn.metrics

import lib.cpoint as cp
import lib.aggregator as ag
import scipy.constants as constant

from lib import logger
from collections import namedtuple
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.naive_bayes import GaussianNB

ClassifierFactory = namedtuple('ClassifierFactory', [ 'construct', 'kwargs' ])

class Classifier(machine.Machine):
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
            self.probs = machine.ClsProbs(True, clf.predict_proba(x))
        except AttributeError as err:
            # this generally happens if a classifier doesn't natively
            # support probability estimates (such as SVMs; set
            # 'probability' in this case)
            self.log.warning(err)

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
                err = msg.format(node, i.min(), i.max(), nans, len(i))
                raise ValueError(err)
            means.append(series.mean())
        (l, r) = means

        gap = right.min() - left.max()
        duration = gap.total_seconds() / constant.minute
        
        label = self.jam_classifier.classify(duration, l, r)
        
        return [ int(label) ]

    def legal_stratification(self, strat):
        return strat.classes.size == self.jam_classifier.categories
