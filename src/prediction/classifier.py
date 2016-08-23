import sklearn.metrics

import lib.cpoint as cp
import lib.aggregator as ag
import scipy.constants as constant

from lib import logger
from machine import Machine
from collections import namedtuple
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.naive_bayes import GaussianNB

ClassifierFactory = namedtuple('ClassifierFactory', [ 'construct', 'kwargs' ])

class Classifier(Machine):
    def __init__(self, observations):
        super().__init__(observations)
        
        self.metrics = [
            self.confusion_matrix,
            self.roc,
            sklearn.metrics.accuracy_score,
            sklearn.metrics.f1_score,
            sklearn.metrics.matthews_corrcoef,
        ]

        self.machines = {
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
            self.probabilities = clf.predict_proba(x)
        except AttributeError as err:
            # this generally happens if a classifier doesn't natively
            # support probability estimates (such as SVMs; set
            # 'probability' to True in this case)
            self.log.warning(err)
        
    def roc(self, y_true, y_pred=None):
        if self.probabilities is None:
            return
        
        p = self.probabilities
        try:
            (fpr, tpr, _) = sklearn.metrics.roc_curve(y_true, p[:,1])
            return sklearn.metrics.auc(fpr, tpr)
        except IndexError:
            msg = 'Invalid probability matrix: {0}'
            raise ValueError(msg.format(p.shape))
                                                        
    def confusion_matrix(self, y_true, y_pred):
        cm = sklearn.metrics.confusion_matrix(y_true, y_pred)
        cv = cm.flatten()
        assert(len(cv) == self.no_labels ** 2)

        return ' '.join(map(str, cv))
