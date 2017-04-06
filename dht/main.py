#!/usr/bin/python

import sys
from static_node import StaticNode

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
        node.run()
        
        while True:
            try:
                cmd = raw_input()
                args = cmd.split(' ')
                print getattr(node, args[0])(*args[1:])
            except Exception as e:
                print 'fucked up son: '+e.message
    elif sys.argv[1] == '--dynamic':
        pass
