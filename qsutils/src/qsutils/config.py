import logging


def get_logger(prefix: str):
    log = logging.getLogger(prefix)
    log.propagate = False
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    ch.setFormatter(formatter)
    log.addHandler(ch)
    return log
