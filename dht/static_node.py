import threading

from node import Node
from node_proxy import NodeProxy


class StaticNode(Node):
    def __init__(self, next_node_id, next_node_ip, *args, **kwargs):
        super(StaticNode, self).__init__(*args, **kwargs)
        self.next_node = NodeProxy(next_node_ip, self.node.port, next_node_id)

    def run(self):
        server_thread = threading.Thread(target=self.tcp_server.serve_forever)
        server_thread.start()
        self._notify_next()
        self._set_second_next_node()

    def _set_second_next_node(self):
        pass

    def _notify_next(self):
        self.next_node.set_prev_node(
            prev_node_ip=self.node.ip,
            prev_node_id=self.node.id_number
        )
