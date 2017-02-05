from pathlib import Path

class Window:
    def __init__(self, observation=1, offset=1):
        self.observation = observation
        self.offset = offset

    def __len__(self):
        return self.offset + self.observation + 1

    def __repr__(self):
        (left, right, empty, clash) = ('l', 'r', '-', '*')

        string = [ empty ] * len(self)
        for i in (0, self.observation):
            string[i] = left
        for i in (-1, -self.observation - 1):
            string[i] = right if string[i] is empty else clash

        return ''.join(string)

    def __str__(self):
        return str(self.topath())

    @classmethod
    def from_path(cls, path):
        components = map(int, path.parts[-2:])

        return cls(*components)
    
    def topath(self):
        parts = (self.observation, self.offset)

        return Path(*[ '{0:02d}'.format(x) for x in parts ])

    def split(self, seq):
        yield from [ seq[:self.observation], seq[-self.observation:] ]
