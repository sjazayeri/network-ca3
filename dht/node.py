import json
import os
import hashlib
import threading
from SocketServer import ThreadingTCPServer, BaseRequestHandler
from collections import defaultdict

import settings
from node_proxy import NodeProxy
from utils import setup_logger, get_file_key


class Node(object):
    ALLOWED_ACTIONS = {
        'set_prev_node',
        'store',
        'query',
        'query_response',
        'join',
        'set_second_next',
        'get_prev_node',
        'get_next_node',
        'get_second_next_node',
        'get_smaller_key_values',
        'get_chunk',
        
        'get_network_graph_cmd',
        'register_file_cmd',
        'download_file_cmd'
    }
    
    class RequestHandler(BaseRequestHandler):
        def __init__(self, node, *args, **kwargs):
            self.node = node
            BaseRequestHandler.__init__(self, *args, **kwargs)

        def handle(self):
            data = self.request.recv(settings.MAX_MESSAGE_LEN)
            request = json.loads(data)
            
            response = self.node.dispatch(request)
            if response is None:
                response = {}

            message = json.dumps(response) if \
                isinstance(response, dict) else response

            self.request.sendall(message)

        @classmethod
        def handler_factory(cls, node):
            def create_handler(*args, **kwargs):
                return cls(node, *args, **kwargs)

            return create_handler

    def __init__(self, work_dir, ip, port, id_number=None):
        self.logger = setup_logger('%s:%d' % (ip, port))
        self.dictionary = defaultdict(lambda: [])
        self.received_data = dict()
        self.received = threading.Event()
        self.work_dir = work_dir
        self.tcp_server = ThreadingTCPServer(
            (ip, port),
            Node.RequestHandler.handler_factory(self)
        )
        self.node = NodeProxy(ip, port, id_number)
        self.prev_node = None
        self.next_node = None
        self.second_next_node = None

        self.file_paths = dict()
        
        self.logger.debug('constructed node')
        
    def dispatch(self, request):
        action = request.pop('action')

        if action in self.ALLOWED_ACTIONS:
            try:
                return getattr(self, action)(**request)
            except Exception as e:
                self.logger.error(e.message)
                return {'details': "bad request"}
        else:
            return {'details': 'invalid method'}
        
    def set_prev_node(self, prev_node_ip, prev_node_port, prev_node_id):
        self.logger.debug(
            'setting previous node to:\nip: %s\nport: %d\nid: %d' %
            (prev_node_ip, prev_node_port, prev_node_id)
        )
        self.prev_node = NodeProxy(prev_node_ip, prev_node_port, prev_node_id)
        
    def _store_local(self, key, value):
        self.logger.debug("setting %d: %s" % (key, str(value)))
        self.dictionary[key].append(value)

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
        if key > self.node.id_number:
            if self.node.id_number > self.next_node.id_number:
                return 'local'
            return 'next'
        elif key < self.prev_node.id_number < self.node.id_number:
            return 'previous'
        else:
            self.logger.debug(
                'self_id: %d\nprevious_id: %d\nrequested: %d\nresult: local' %
                (self.node.id_number, self.prev_node.id_number, key)
            )
            return 'local'
    
    def query(self, key, recipient_ip, recipient_port):
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
        self.received_data[key] = value
        self.received.set()
        self.logger.debug('received data '+str(key)+": "+str(value))
        
    def join(self, id_number, recipient_ip, recipient_port):
        self.logger.debug(
            'join with:\nid: %d\nip: %s\nport: %d'
            % (id_number, recipient_ip, recipient_port)
        )
        recipient = NodeProxy(recipient_ip, recipient_port, id_number)
        if id_number == self.node.id_number:
            self.logger.debug('error, duplicate id')
            recipient.join_response_failure(message='id in use')
        elif id_number < self.node.id_number:
            self.logger.debug('passing join to prev_node')
            self.prev_node.join(
                id_number=id_number,
                recipient_ip=recipient_ip,
                recipient_port=recipient_port
            )
        elif id_number > self.next_node.id_number:
            self.logger.debug('passing join to next_node')
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
        tmp_second_next_node = self.second_next_node
        self.second_next_node = self.next_node
        self.next_node = NodeProxy(recipient_ip, recipient_port, id_number)

        self.next_node.join_response_success(
            id_number=id_number,
            prev_ip=self.node.ip,
            prev_port=self.node.port,
            prev_id=self.node.id_number,
            next_ip=tmp_next_node.ip,
            next_port=tmp_next_node.port,
            next_id=tmp_next_node.id_number,
            second_next_ip=tmp_second_next_node.ip,
            second_next_port=tmp_second_next_node.port,
            second_next_id=tmp_second_next_node.id_number
        )

    def set_second_next(self, second_next_ip, second_next_port,
                        second_next_id):
        self.second_next_node = NodeProxy(second_next_ip, second_next_port,
                                          second_next_id)

    def get_prev_node(self):
        return self.prev_node.to_dict()
        
    def get_next_node(self):
        return self.next_node.to_dict()

    def get_second_next_node(self):
        return self.second_next_node.to_dict()
        
    def get_network_graph_cmd(self):
        graph = dict()
        visited_ids = set()
        current_node = self.node
        while current_node.id_number not in visited_ids:
            visited_ids.add(current_node.id_number)
            prev_node = NodeProxy(**current_node.get_prev_node())
            next_node = NodeProxy(**current_node.get_next_node())
            second_next_node = NodeProxy(**current_node.get_second_next_node())
            graph[current_node.id_number] = {
                'prev_node': prev_node.id_number,
                'next_node': next_node.id_number,
                'second_next_node': second_next_node.id_number
            }
            current_node = next_node

        return graph

    def get_smaller_key_values(self, key):
        self.logger.debug('get_smaller_key_values called with key: %d' % key)
        smaller_dict = {k: v for k, v in self.dictionary.iteritems()
                        if k < key}

        self.logger.debug('\ndictionary: %s\nsmaller_dict: %s' %
                          (str(self.dictionary), str(smaller_dict)))
    
        return smaller_dict

    def query_cmd(self, key):
        return self.query(int(key), self.node.ip, self.node.port)

    def store_cmd(self, key, value):
        return self.store(int(key), value)

    def register_file_cmd(self, file_path):
        file_path = os.path.join(self.work_dir, file_path)
        filename = os.path.split(file_path)[1]

        self.file_paths[filename] = file_path
        
        filename_hex_digest = hashlib.md5(filename).hexdigest()
        
        n_chunks = 0
        file_md5 = hashlib.md5()
        with open(file_path, 'r') as f:
            while True:
                data = f.read(settings.CHUNK_SIZE)
                if not data:
                    break
                n_chunks += 1
                file_md5.update(data)
                
        file_hex_digest = file_md5.hexdigest()
        with open(file_path + '.torrent', 'w') as f:
            file_content = map(str, [filename, filename_hex_digest,
                                     n_chunks, file_hex_digest])
            f.write('\n'.join(file_content))

        file_key = get_file_key(filename, settings.KEY_MOD)

        self.store_cmd(
            file_key,
            {
                'filename': filename,
                'ip': self.node.ip,
                'port': self.node.port
            }
        )

    def get_chunk(self, filename, chunk_number):
        file_path = self.file_paths[filename]
        self.logger.debug(file_path)
        with open(file_path, 'rb') as f:
            f.seek(chunk_number*settings.CHUNK_SIZE)
            data = f.read(settings.CHUNK_SIZE)

        self.logger.debug("sending chunk %d" % chunk_number)
        self.logger.debug("encoding")
        return data

    def download_file_cmd(self, torrent_file_path):
        torrent_file_path = os.path.join(self.work_dir, torrent_file_path)
        with open(torrent_file_path, 'r') as f:
            lines = f.readlines()

        self.logger.debug(lines)
            
        filename = lines[0].strip()
        n_chunks = int(lines[2])

        file_key = get_file_key(filename, settings.KEY_MOD)

        self.received.clear()
        self.query_cmd(file_key)
        self.received.wait()

        self.received.clear()
        
        possible_peers = self.received_data[file_key]

        peers = [NodeProxy(value['ip'], value['port']) for value in
                 possible_peers if value['filename'] == filename]

        if not peers:
            raise ValueError("file not found")

        file_data = []
        current_peer = 0
        for i in xrange(n_chunks):
            self.logger.debug('receiving chunk %d' % i)
            response = peers[current_peer].get_chunk(
                response_type='raw_response',
                filename=filename,
                chunk_number=i
            )
            file_data.append(response)
            self.logger.debug("received chunk %d" % i)
            current_peer = (current_peer+1) % len(peers)

        file_path = os.path.join(self.work_dir, filename)
        with open(file_path, 'wb') as f:
            f.write(''.join(file_data))

        self.register_file_cmd(filename)
