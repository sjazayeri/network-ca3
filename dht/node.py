import copy
import json
import socket
from SocketServer import ThreadingTCPServer, BaseRequestHandler
from datetime import datetime

from ..utils import setup_logger
import ..settings

class Node(object):
    ALLOWED_ACTIONS = {'set_previous'}
    
    class RequestHandler(BaseRequestHandler):
        def __init__(self, node, *args, **kwargs):
            self.node = node
            BaseRequestHandler.__init__(self, *args, **kwargs)

        def handle(self):
            self.node.logger.debug("new connection")
            
            data = self.request.recv(settings.MAX_MESSAGE_LEN)
            request = json.loads(data)
            
            response = self.node.dispatch(request)
            if response is None:
                response = {}

            self.request.sendall(json.dumps(response))

            self.node.logger.debug("connection closed")

        @classmethod
        def handler_factory(cls, node):
            def create_handler(*args, **kwargs):
                return cls(node, *args, **kwargs)

            return create_handler

    def __init__(self, ip, port, id_number):
        self.logger = setup_logger(str(id_number))
        self.dictionary = dict()
        self.id_number = id_number
        self.ip = ip
        self.port = port
        self.tcp_server = ThreadingTCPServer(
            (host, port),
            Node.RequestHandler.handler_factory(self)
        )

    def dispatch(self, request):
        action = request.pop('action')

        if action in self.ALLOWED_ACTIONS:
            try:
                return getattr(self, action)(**request)
            except Exception as e:
                self.logger.error(str(self.id_number)+" "+str(e.message))
                return {'details': "bad request"}
        
    def set_previous(self, prevnode_id, prevnode_ip):
        self.prevnode_id = prevnode_id
        self.prevnode_ip = prevnode_ip

    def _store_local(self, key, value):
        self.dictionary[key] = value

    def store(self, key, value):
        direction = self.get_movement_direction(key)
        if direction == 'local':
            self._store_local(key, value)
        elif direction == 'next':
            call_remote_function((self.nextnode_ip, self.port),
                                 'store', key=key, value=value)
        else:
            call_remote_function((self.prevnode_ip, self.port),
                                 'store', key=key, value=value)

    def _retrieve_local(self, key):
        return self.dictionary[key]

    def get_movement_direction(self, key):
        if key > self.id_number:
            if self.id_number > self.nextnode_id:
                return 'local'
            return 'next'
        elif key < self.prevnode_id:
            return 'previous'
        else:
            return 'local'
    
    def query(self, key, recipient_ip):
        direction = self.get_movement_direction(key)
        if direction == 'local':
            call_remote_function((recipient_ip, self.port),
                                 'query_response', key=key, value=value)
        elif direction == 'next':
            call_remote_function((self.nextnode_ip, self.port),
                                 'query', key=key, recipient_ip=recipient_ip)
        else:
            call_remote_function((self.prevnode_ip, self.port),
                                 'query', key=key, recipient_ip=recipient_ip)
                
    def query_response(self, key, value):
        print 'received data'+str(key)+": "+str(value)

    def ping(self):
        pass
