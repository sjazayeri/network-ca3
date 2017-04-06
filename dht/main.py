#!/usr/bin/python

import sys
from static_node import StaticNode
from dynamic_node import DynamicNode

if __name__ == '__main__':
    if sys.argv[1] == '--static':
        next_ip = sys.argv[2]
        next_port = int(sys.argv[3])
        next_id = int(sys.argv[4])
        self_ip = sys.argv[5]
        self_port = int(sys.argv[6])
        self_id = int(sys.argv[7])
        node = StaticNode(next_ip, next_port, next_id, self_ip, self_port, self_id)
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
    
