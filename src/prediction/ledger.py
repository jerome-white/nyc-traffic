import csv
from collections import namedtuple

Entry = namedtuple('Entry', 'ini, segment, event')

class Ledger(dict):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.fp.close()
        
    def __init__(self, ledger, node, init=False):
        super().__init__()
        
        if ledger.is_dir():
            for i in ledger.glob('*.csv'):
                with i.open() as fp:
                    reader = csv.reader(fp)
                    for (ini, segment, event, status) in reader:
                        entry = Entry(ini, int(segment), event)
                        self[entry] = int(status)
        elif init:
            ledger.mkdir(parents=True, exist_ok=True)

        output = ledger.joinpath(str(node)).with_suffix('.csv')
        self.fp = output.open('a')
        self.writer = csv.writer(self.fp)
        
    def record(self, entry, result):
        self[entry] = int(result)
        
        self.writer.writerow(list(entry) + [ self[entry] ])
        self.fp.flush()
