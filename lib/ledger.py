import csv
import inspect
import operator as op

class Ledger(dict):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.fp.close()

    def __init__(self, ledger, segment, entry_factory):
        super().__init__()
        
        if ledger.is_dir():
            for i in ledger.glob('*.csv'):
                with i.open() as fp:
                    reader = csv.reader(fp)
                    for (*data, status) in reader:
                        entry = entry_factory(*data)
                        self[entry] = int(status)
        else:
            ledger.mkdir(parents=True, exist_ok=True)

        output = ledger.joinpath(str(segment)).with_suffix('.csv')
        self.fp = output.open('a', buffering=1)
        self.writer = csv.writer(self.fp)

    def record(self, entry, result=True):
        self[entry] = int(result)
        self.writer.writerow(list(entry) + [ self[entry] ])
