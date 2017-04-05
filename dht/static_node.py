import threading

from node import Node
from utils import call_remote_function


class StaticNode(Node):
    def __init__(self, next_peer_id, next_peer_ip, *args, **kwargs):
        super(StaticNode, self).__init__(*args, **kwargs)
        self.next_peer_id = next_peer_id
        self.next_peer_ip = next_peer_ip
        
    def run(self):
        server_thread = threading.Thread(target=self.tcp_server.serve_forever)
        server_thread.start()
        self.notify_next()
        
    def notify_next(self):
        response = call_remote_function(
            (self.next_peer_ip, self.port),
            function='set_previous',
            prevnode_id=self.id_number,
            prevnode_ip=self.ip
        )
        self.second_nextnode_id = response['nextnode_id']
        self.second_nextnode_ip = response['nextnode_ip']
