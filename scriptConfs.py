import sys
import os

devDebug = False



def getConf(logName):
    dictConf = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
                'datefmt': '%Y-%m-%dT%H:%M:%S',
            },
        },
        'handlers': {
            'default': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
                'stream': sys.stderr,
            },
            'rotating_to_file': {
                'level': 'DEBUG',
                'class': "logging.handlers.RotatingFileHandler",
                'formatter': 'standard',
                "filename": logName,
                "maxBytes": 10000000,
                "backupCount": 2,
            },
        },
        'loggers': {
            '': {
                'handlers': ['default', 'rotating_to_file'],
                'level': 'INFO',
                'propagate': True
            }
        }
    }
    return dictConf
