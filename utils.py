import logging
import json
import threading
import socket
import time


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

def call_remote_function(host, function, max_tries=3, delay=1, backoff=5, **kwargs):
    tries = 0
    while tries < max_tries:
        try:
            connection = socket.connection(host)
            kwargs['action'] = function
            connection.sendall(json.dumps(kwargs))
            response = connection.recv(settings.MAX_MESSAGE_LEN)
            connection.close()
            return json.loads(response)
        except:
            tries += 1
            time.sleep(delay)
            delay += backoff
            
    raise IOError('remore function call failed')

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
