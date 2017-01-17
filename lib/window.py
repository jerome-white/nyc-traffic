class Window:
    def __init__(self, observation=1, offset=1):
        self.observation = observation
        self.offset = offset

    def __len__(self):
        return self.offset + self.observation

    def split(self, seq):
        for i in (0, self.offset):
            j = i + self.observation
            yield seq[i:j]
