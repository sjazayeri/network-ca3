#!/usr/bin/python

import sys
from dht import StaticNode, DynamicNode
from config import STATIC_NODES


def _serve_command(server_node):
    while True:
        try:
            args = raw_input().split(' ')
            result = getattr(server_node, args[0])(*args[1:])
            if result:
                print result
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
    elif sys.argv[1] == '--dynamic':
        node = DynamicNode(
            node_list=STATIC_NODES,
            ip=sys.argv[2],
            port=int(sys.argv[3]),
            id_number=int(sys.argv[4])
        )
    else:
        print "Wrong flag!!"
        sys.exit()

    try:
        node.run()
    except EnvironmentError:
        sys.exit()

    _serve_command(node)
