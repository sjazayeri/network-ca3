import json
from SocketServer import ThreadingTCPServer, BaseRequestHandler

import settings
from node_proxy import NodeProxy
from utils import setup_logger


class Node(object):
    ALLOWED_ACTIONS = {
        'set_prev_node',
        'store',
        'query',
        'query_response',
        'join',
        'set_second_next',
        'get_next_node'
    }
    
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
        self.tcp_server = ThreadingTCPServer(
            (ip, port),
            Node.RequestHandler.handler_factory(self)
        )
        self.node = NodeProxy(ip, port, id_number)
        self.prev_node = None
        self.next_node = None
        self.second_next_node = None
        
    def dispatch(self, request):
        action = request.pop('action')

        if action in self.ALLOWED_ACTIONS:
            try:
                return getattr(self, action)(**request)
            except Exception as e:
                self.logger.error(str(self.node.id_number)+" "+str(e.message))
                return {'details': "bad request"}
        
    def set_prev_node(self, prev_node_ip, prev_node_port, prev_node_id):
        self.prev_node = NodeProxy(prev_node_ip, prev_node_port, prev_node_id)
        
    def _store_local(self, key, value):
        self.dictionary[key] = value

    def store(self, key, value):
        direction = self._get_movement_direction(key)
        self.logger.debug("start store action -> %s" % direction)
        if direction == 'local':
            self._store_local(key, value)
        elif direction == 'next':
            self.next_node.store(key=key, value=value)
        else:
            self.prev_node.store(key=key, value=value)

    def _retrieve_local(self, key):
        return self.dictionary[key]

    def _get_movement_direction(self, key):
        key = int(key)
        if key > self.node.id_number:
            if self.node.id_number > self.next_node.id_number:
                return 'local'
            return 'next'
        elif key < self.prev_node.id_number < self.node.id_number:
            return 'previous'
        else:
            return 'local'
    
    def query(self, key, recipient_ip, recipient_port):
        recipient_port = int(recipient_port)
        direction = self._get_movement_direction(key)
        self.logger.debug("start query action -> %s" % direction)
        if direction == 'local':
            NodeProxy(
                recipient_ip,
                recipient_port
            ).query_response(key=key, value=self._retrieve_local(key))
        elif direction == 'next':
            self.next_node.query(
                key=key,
                recipient_ip=recipient_ip,
                recipient_port=recipient_port
            )
        else:
            self.prev_node.query(
                key=key,
                recipient_ip=recipient_ip,
                recipient_port=recipient_port
            )
                
    def query_response(self, key, value):
        self.logger.debug('received data '+str(key)+": "+str(value))

    def join(self, id_number, recipient_ip, recipient_port):
        recipient = NodeProxy(recipient_ip, recipient_port, id_number)
        if id_number == self.node.id_number:
            recipient.join_response_failure(message='id in use')
        elif id_number < self.node.id_number:
            self.prev_node.join(
                id_number=id_number,
                recipient_ip=recipient_ip,
                recipient_port=recipient_port
            )
        elif id_number > self.next_node.id_number:
            self.next_node.join(
                id_number=id_number,
                recipient_ip=recipient_ip,
                recipient_port=recipient_port
            )
        else:
            self._add_to_network(id_number, recipient_ip, recipient_port)
            
    def _add_to_network(self, id_number, recipient_ip, recipient_port):
        self.prev_node.set_second_next(
            second_next_ip=recipient_ip,
            second_next_port=recipient_port,
            second_next_id=id_number
        )
        self.next_node.set_prev_node(
            prev_node_ip=recipient_ip,
            prev_node_port=recipient_port,
            prev_node_id=id_number
        )

        tmp_next_node = self.next_node
        self.second_next_node = self.next_node
        self.next_node = NodeProxy(recipient_ip, recipient_port, id_number)

        self.next_node.join_response_success(
            prev_ip=self.node.ip,
            prev_port=self.node.port,
            prev_id=self.node.id_number,
            next_ip=tmp_next_node.ip,
            next_port=tmp_next_node.port,
            next_id=tmp_next_node.id_number,
            second_next_ip=self.next_node.ip,
            second_next_port=self.next_node.port,
            second_next_id=self.next_node.id_number
        )

    def set_second_next(self, second_next_ip, second_next_port,
                        second_next_id):
        self.second_next_node = NodeProxy(second_next_ip, second_next_port,
                                          second_next_id)

    def get_next_node(self):
        return {
            'ip': self.next_node.ip,
            'port': self.next_node.port,
            'id_number': self.next_node.id_number
        }
