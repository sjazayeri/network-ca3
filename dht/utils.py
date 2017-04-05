import logging
import json
import threading


def read_json_file(path):
    with open(path, 'r') as f:
        data = ''.join(f.readlines())
    return json.loads(data)


def setup_logger(name):
    formatter = logging.Formatter(
        fmt="%(levelname)s - %(name)s - %(message)s"
    )

    file_handler = logging.FileHandler("%s.log" % name)
    file_handler.setFormatter(formatter)
    
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(file_handler)
    return logger


class MutexCounter(object):
    def __init__(self, initial_value=0):
        self.value = initial_value
        self.mutex = threading.Lock()

    def increment(self):
        self.mutex.acquire()
        self.value += 1
        self.mutex.release()

    def get_and_reset(self, value=0):
        self.mutex.acquire()
        result = self.value
        self.value = value
        self.mutex.release()
        return result
