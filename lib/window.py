ames = [ 'observation', 'prediction', 'target' ]
        
class Window:
    def __init__(self, observation, prediction, target):
        self.observation = observation
        self.prediction = prediction
        self.target = target

        self.__elements = []

    def __len__(self):
        return self.observation + self.prediction + self.target

    def __repr__(self):
        m = map(str, [ self.observation, self.prediction, self.target ])
        return ','.join(m)

    def __str__(self):
        return repr(self)

    def __list__(self):
        return [ self.observation, self.prediction, self.target ]

    def __iter__(self):
        self.__elements = [ self.observation, self.prediction, self.target ]
        return self

    def __next__(self):
        if self.__elements:
            return self.__elements.pop(0)
        
        raise StopIteration
    
    def tail(self):
        return Window(self.target, self.prediction, self.target)

def from_config(config):
    w = [ 'observation', 'prediction', 'target' ]
    
    return Window(*[ int(config['window'][x]) for x in w ])
    
