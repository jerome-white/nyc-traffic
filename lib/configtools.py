from collections import OrderedDict

def ordered(config):
    sections = [
        'window',
        'machine',
        'neighbors',
        'parameters',
    ]
    assert(all([ x in config for x in sections ]))
        
    ordered = OrderedDict()
    for i in sections:
        d = config[i]
        for (key, value) in sorted(d.items()):
            assert(key not in ordered) # same key, different sections
            ordered[key] = value

    return ordered
