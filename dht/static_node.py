import threading

from node import Node
from node_proxy import NodeProxy


class StaticNode(Node):
    def __init__(self, next_node_ip, next_node_port, next_node_id, *args,
                 **kwargs):
        super(StaticNode, self).__init__(*args, **kwargs)
        self.next_node = NodeProxy(next_node_ip, next_node_port, next_node_id)

    def run(self):
        server_thread = threading.Thread(target=self.tcp_server.serve_forever)
        server_thread.daemon = True
        server_thread.start()
        try:
            self._notify_next()
            self._set_second_next_node()
        except IOError as e:
            self.logger.error("closing node: " + e.message)
            raise EnvironmentError()

    def _set_second_next_node(self):
        self.logger.debug("start set second next node")
        response = self.next_node.get_next_node()
        self.second_next_node = NodeProxy(**response)
        self.logger.debug("second next node: %d" %
                          self.second_next_node.id_number)

    def _notify_next(self):
        self.logger.debug("start notify next")
        self.next_node.set_prev_node(
            prev_node_ip=self.node.ip,
            prev_node_port=self.node.port,
            prev_node_id=self.node.id_number
        )
