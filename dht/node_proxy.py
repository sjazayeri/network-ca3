from utils import call_remote_function

class NodeProxy(object):
    def __init__(self, ip, port, id_number=None):
        self.ip = ip
        self.port = port
        self.id_number = id_number

    def __getattr__(self, method):
        def remote_method(*args, **kwargs):
            return call_remote_function(
                (self.ip, self.port),
                method,
                *args,
                **kwargs
            )
        return remote_method
