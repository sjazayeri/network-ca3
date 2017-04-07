import threading
import time
from random import randint

from node import Node
from node_proxy import NodeProxy


class DynamicNode(Node):
    ALLOWED_ACTIONS = Node.ALLOWED_ACTIONS.union({
        'join_response_success',
        'join_response_failure'
    })

    def __init__(self, node_list, *args, **kwargs):
        super(DynamicNode, self).__init__(*args, **kwargs)

        self.node_list = node_list
        self.joined = False
        self.wait_for_join_response = None

    def run(self):
        server_thread = threading.Thread(target=self.tcp_server.serve_forever)
        server_thread.daemon = True
        server_thread.start()
        self._join_network()

    def _join_network(self):
        while not self.joined:
            node_index = randint(0, len(self.node_list)-1)
            selected_node = NodeProxy(*self.node_list[node_index])
            _, _, next_id_number = self.node_list[(node_index+1)%len(self.node_list)]

            my_id = randint(*sorted((selected_node.id_number+1, next_id_number)))

            self.wait_for_join_response = True
            try:
                selected_node.join(
                    id_number=my_id,
                    recipient_ip=self.node.ip,
                    recipient_port=self.node.port
                )
            except IOError:
                self.logger.error("static node %s not responding" %
                                  selected_node.id_number)
                self.wait_for_join_response = False

            while self.wait_for_join_response:
                time.sleep(1)

        self.retrieve_data()
                
    def retrieve_data(self):
        data = self.next_node.get_smaller_key_values(key=self.node.id_number)
            
    def join_response_success(self, id_number,
                              prev_ip, prev_port, prev_id, next_ip,
                              next_port, next_id, second_next_ip,
                              second_next_port, second_next_id):
        self.logger.debug("join successful, id: %d"%id_number)
        self.node.id_number = id_number
        self.prev_node = NodeProxy(prev_ip, prev_port, prev_id)
        self.next_node = NodeProxy(next_ip, next_port, next_id)
        self.second_next_node = NodeProxy(second_next_ip, second_next_port,
                                          second_next_id)
        self.joined = True
        self.wait_for_join_response = False

    def join_response_failure(self, message):
        self.logger.debug("join failed because "+message)
        self.wait_for_join_response = False
