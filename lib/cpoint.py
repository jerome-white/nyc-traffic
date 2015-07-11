import scipy.constants as constant

########################################################################

def to_gravity(duration, before, after):
    '''
    Assume x and y to be miles per hour, and t to be minutes
    '''
    meters = (after - before) * constant.mile # miles to meters
    seconds = duration * constant.minute # minutes to seconds
    acceleration = meters / constant.hour / seconds
    
    return acceleration / constant.g

def compare(measurement, threshold, direction):
    allowable = measurement <= threshold
    
    return allowable if direction else not allowable

def changed(duration, before, after, threshold):
    if threshold < 0:
        allowable = to_gravity(duration, before, after) <= threshold
    else:
        allowable = to_gravity(duration, after, before) > threshold

    return allowable

def bucket(tstamp):
    assert(tstamp.second == 0)
    return tstamp.hour * 60 + tstamp.minute

def aoc(distribution):
    keys = len(distribution.keys())
    assert(keys > 0)
    area = [ sum(i) for i in distribution.values() ]
    
    return sum(area) / keys
