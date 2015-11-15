import logging

# Level    Numeric value
# CRITICAL 50
# ERROR    40
# WARNING  30
# INFO     20
# DEBUG    10
# NOTSET   0

class Logger:
    __instance = None
    __level = logging.DEBUG

    def __new__(self):
        if not self.__instance:
            fmt = [
                '%(levelname)s:%(asctime)s',
                '%(process)d',
                '%(filename)s:%(lineno)d',
                '%(message)s'
            ]
            logging.basicConfig(level=self.__level, format=' '.join(fmt))
            self.log = logging

            self.__instance = super(Logger, self).__new__(self)
            
        return self.__instance

log = Logger().log
