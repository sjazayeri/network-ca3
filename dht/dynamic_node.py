import threading
import time
from random import randint

from node import Node
from utils import call_remote_function


class DynamicNode(Node):
    def __init__(self, node_list, *args, **kwargs):
        super(DynamicNode, self).__init__(*args, **kwargs)

        self.node_list = node_list
        self.joined = False
        self.wait_for_join_response = None

    def run(self):
        server_thread = threading.Thread(target=self.tcp_server.serve_forever)
        server_thread.start()
        self._join_network()
        
    def _join_network(self):
        while not self.joined:
            node_index = randint(0, len(self.node_list))
            selected_ip, selected_port, selected_id =\
                self.node_list[node_index]
            _, _, next_id = self.node_list[node_index+1]
            
            my_id = randint(selected_id+1, next_id)

            self.wait_for_join_response = True
            call_remote_function((selected_ip, selected_port),
                                 'join', id_number=my_id, recipient_ip=self.ip)

            while self.wait_for_join_response:
                time.sleep(1)

    def join_response_success(self, previous_ip, previous_id, next_ip,
                              next_id, second_next_ip, second_next_id):
        self.prevnode_ip = previous_ip
        self.prevnode_id = previous_id
        self.nextnode_ip = next_ip
        self.nextnode_id = next_id
        self.second_nextnode_ip = second_next_ip
        self.second_nextnode_id = second_next_id

        self.joined = True
        self.wait_for_join_response = False

    def join_response_failure(self, message):
        self.logger.debug("join failed because "+message)
