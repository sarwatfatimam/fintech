
import logging
import logging.config


logconfig = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] %(levelname)s %(module)s:%(funcName)s:%(lineno)d - %(message)s',
            'datefmt': '%m/%d %H:%M:%S'
        },
    },
    'handlers': {
        'default': {
            'class': "logging.StreamHandler",
            'stream': 'ext://sys.stdout',
            'formatter': 'default'},
        'filer': {
            'class': "logging.handlers.RotatingFileHandler",
            'filename': 'etl.log',
            'maxBytes': 512 * 1024,
            'backupCount': 3,
            'formatter': 'default'
        }
    },
    'root': {
        'handlers': ['default', 'filer'],
        'level': 'INFO'}
}


logging.config.dictConfig(logconfig)


def exception(e):
    logging.exception(e)


def info(i):
    logging.info(i)


def warning(i):
    logging.warning(i)
