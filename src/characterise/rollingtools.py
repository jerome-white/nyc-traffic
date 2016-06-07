import numpy as np

from collections import namedtuple

def apply(df, window, classifier):
    '''
    determine whether a window constitutes a traffic event
    '''
    assert(type(df) == np.ndarray)

    segments = (df[:window.observation], df[-window.target:])
    left_right = []
    for i in segments:
        if np.isnan(np.sum(i)):
            return np.NaN
        left_right.append(i.mean())

    return classifier.classify(window.prediction, *left_right)

CharArgs = namedtuple('CharArgs', [ 'classifier', 'window', 'roller' ])
def mkargs(config):
    node = nd.Node(nid)
    
    threshold = float(config['parameters']['acceleration'])    
    classifier = cp.Acceleration(threshold)
    
    window = win.from_config(config)

    roller = node.speed.readings.rolling(len(window), center=True)    

    return CharArgs(node, classifier, window, roller)
