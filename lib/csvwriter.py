import sys

from csv import DictWriter

class CSVWriter:
    def __enter__(self):
        return self.writer

    def __exit__(self, type, value, tb):
        if self.fp != sys.stdout:
            self.fp.close()
        
    def __init__(self, fieldnames, fname=None, delimiter=','):
        self.fp = open(fname, mode='w') if fname else sys.stdout
        self.writer = DictWriter(self.fp, fieldnames, delimiter=delimiter)
