import warnings
from collections import namedtuple

import numpy as np
import sklearn.metrics
from sklearn.metrics.base import UndefinedMetricWarning
from sklearn.cross_validation import StratifiedShuffleSplit
from sklearn.svm import SVC, SVR
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.naive_bayes import GaussianNB

from lib import logger

Data = namedtuple('Data', 'x_train, x_test, y_train, y_test')
ClassifierFactory = namedtuple('ClassifierFactory', [ 'construct', 'kwargs' ])

Selector = lambda x: {
    'classification': Classifier,
    'estimation': Estimator,
}[x]

class Machine:
    def __init__(self, observations):
        assert(np.isfinite(observations).all())

        self.features = observations[:,:-1]
        self.labels = observations[:,-1:].ravel()
        self.no_labels = len(np.unique(self.labels))

        self.metrics = []
        self.machines = {}
        self.probabilities = None
        
    def stratify(self, folds, test_size=0.2):
        assert(0 < test_size < 1)

        try:
            stratifications = StratifiedShuffleSplit(self.labels, n_iter=folds,
                                                     test_size=test_size)
        except ValueError:
            raise StopIteration

        for (train, test) in stratifications:
            yield Data(self.features[train], self.features[test],
                       self.labels[train], self.labels[test])

    def machinate(self, methods):
        wanted = set(methods.split(','))
        for i in wanted.intersection(self.machines.keys()):
            factory = self.machines[i]
            instance = factory.construct(**factory.kwargs)

            yield (factory.construct.__name__, instance)

    def predict(self, data, pred):
        log = logger.getlogger()

        with warnings.catch_warnings():
            warnings.filterwarnings('error')
            for f in self.metrics:
                try:
                    result = f(data.y_test, pred)
                except Exception as error:
                    log.warning(error)
                    result = None
                    
                yield (f.__name__, result)

    def set_probabilities(self, clf, x):
        raise NotImplementedError()

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
            log = logger.getlogger()
            log.warning(err)
        
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
        # assert(len(cv) == self.no_labels ** 2)

        return ' '.join(map(str, cv))

class Estimator(Machine):
    def __init__(self, observations):
        super().__init__(observations)
        
        self.metrics = [
            sklearn.metrics.explained_variance_score,
            sklearn.metrics.mean_absolute_error,
            sklearn.metrics.mean_squared_error,
        ]

        self.machines = {
            'svm': SVR,
            'tree': DecisionTreeRegressor,
            'bayes': GaussianNB,
            'forest': RandomForestRegressor,
        }
