#!/usr/bin/python

import sys
from dht import StaticNode, DynamicNode


def _serve_command():
    while True:
        try:
            args = raw_input().split(' ')
            print getattr(node, args[0])(*args[1:])
        except Exception as e:
            print "Wrong command: " + e.message


if __name__ == '__main__':
    if sys.argv[1] == '--static':
        node = StaticNode(
            next_node_ip=sys.argv[2],
            next_node_port=int(sys.argv[3]),
            next_node_id=int(sys.argv[4]),
            ip=sys.argv[5],
            port=int(sys.argv[6]),
            id_number=int(sys.argv[7])
        )
        try:
            node.run()
        except EnvironmentError:
            sys.exit()

        _serve_command()
    elif sys.argv[1] == '--dynamic':
        node = DynamicNode(node_list=[])
        pass
