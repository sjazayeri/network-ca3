import logging
import json
import socket
import time
import hashlib

import settings


def read_json_file(path):
    with open(path, 'r') as f:
        data = ''.join(f.readlines())
    return json.loads(data)


def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(levelname)s - %(name)s - %(message)s"
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # file_handler = logging.FileHandler("%s.log" % name)
    # file_handler.setFormatter(formatter)
    # logger.addHandler(file_handler)
    return logger


logger = setup_logger(__name__)


def call_remote_function(host, function, response_type='json', time_out=None, max_tries=3, delay=1,
                         backoff=5, **kwargs):
    tries = 0
    while tries < max_tries:
        try:
            connection = socket.create_connection(host)
            if time_out:
                connection.settimeout(time_out)

            kwargs['action'] = function
            connection.sendall(json.dumps(kwargs))
            response = connection.recv(settings.MAX_MESSAGE_LEN)
            connection.close()

            return json.loads(response) if \
                response_type is 'json' else response

        except Exception as e:
            tries += 1
            logger.debug(str(type(e)) + e.message)
            time.sleep(delay)
            delay += backoff
            
    raise IOError('remote function call failed')


def get_file_key(filename, key_mod):
    filename_hexdigest = hashlib.md5(filename).hexdigest()
    filename_hash_value = int(filename_hexdigest, base=16)
    file_key = filename_hash_value%key_mod

    return file_key
