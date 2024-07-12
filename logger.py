import logging

FORMAT = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s')


def _setup_logger(name: str, level='INFO') -> logging.Logger:
    level_dict = {
        'NOTSET': logging.NOTSET,
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL,
        }

    handler_console = logging.StreamHandler()
    handler_console.setFormatter(FORMAT)

    _logger = logging.getLogger(name)
    _logger.setLevel(level_dict[level])
    _logger.addHandler(handler_console)
    return _logger


logger = _setup_logger('logging')
