import csv
import argparse

class CommandLine:
    __type_map = {
        'str': str,
        'int': int,
        # 'bool': bool,
        'float': float,
    }
        
    def __mkarg(arg, prefix='--'):
        return prefix + str(arg)

    def __str__(self):
        d = self.args.__dict__
        args = [ '='.join(map(str, x)) for x in d.items() ]
        
        return ','.join(args)

    def __init__(self, fname, desc=''):
        fp = open(fname)
        reader = csv.DictReader(fp)
        parser = argparse.ArgumentParser(description=desc)
        for line in reader:
            prefix = '--' + line['prefix']
            del line['prefix']
            if line['choices']:
                line['choices'] = line['choices'].split('|')

            # remove mappings that are left blank
            row = {}
            for i in line.keys():
                if line[i]:
                    row[i] = line[i]

            # clean up specific mappings
            if 'type' in row and row['type'] in self.__type_map:
                row['type'] = self.__type_map[row['type']]
                # if row['type'] == int and 'default' in row:
                #     row['default'] = int(row['default'])
                if 'default' in row:
                    row['default'] = row['type'](row['default'])

            # parse
            parser.add_argument(prefix, **row)
        fp.close()
        
        self.args = parser.parse_args()

    def options(self):
        return sorted(self.args.__dict__.keys())

    def values(self):
        return [ self.args.__dict__[i] for i in self.options() ]
        
