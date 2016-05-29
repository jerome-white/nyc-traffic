import numpy as np

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
