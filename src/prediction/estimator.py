import sklearn.metrics

import lib.aggregator as ag

from machine import Machine
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.naive_bayes import GaussianNB

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