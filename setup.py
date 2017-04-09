"""
static1 -> 192.168.1.0,
static2 -> 192.168.1.1
static3 -> 192.168.1.2
static4 -> 192.168.1.3
dynamic1 -> 192.168.1.4
dynamic2 -> 192.168.1.5
"""
from mininet.net import Mininet
from mininet.topo import SingleSwitchTopo
import threading
import time
import os

port_number = 8585

def run_and_print(host, cmd):
    run_thread = threading.Thread(target=host.cmd,
                                 args=[cmd])
    run_thread.daemon = True
    run_thread.start()
    while True:
        print host.stdout.readline()

static_ips = ['192.168.1.0', '192.168.1.1', '192.168.1.2', '192.168.1.3']
static_ids = [100, 200, 300, 400]

dynamic_ips = ['192.168.1.4', '192.168.1.5']

topo = SingleSwitchTopo(k=len(static_ips)+len(dynamic_ips)+1)
net = Mininet(topo=topo)

client_host = net.hosts[-1]
client_host.setIP('192.168.1.10')

static_hosts = [net.hosts[0], net.hosts[1], net.hosts[2], net.hosts[3]]
dynamic_hosts = [net.hosts[4], net.hosts[5]]

for ip, host in zip(static_ips+dynamic_ips, static_hosts+dynamic_hosts):
    host.setIP(ip)

net.start()
net.pingAll()

threads = []

static_n = len(static_ips)
for i in xrange(static_n):
    print "starting static node %d" % static_ids[i]
    template = 'python main.py --static %s %d %d %s %d %d %s'
    current_id = static_ids[i]
    current_ip = static_ips[i]
    next_idx = (i+1)%static_n
    next_id = static_ids[next_idx]
    next_ip = static_ips[next_idx]
    work_dir = os.path.join('workdirs', str(i+1))
    chost_thread = threading.Thread(
            target=run_and_print,
            args=[static_hosts[i], template%(next_ip, port_number,
                next_id, current_ip, port_number, current_id, work_dir)]
            )
    threads.append(chost_thread)
    chost_thread.daemon = True
    chost_thread.start()

time.sleep(10)

dynamic_n = len(dynamic_ips)
for i in xrange(dynamic_n):
    print "starting dynamic node %d" % i
    template = 'python main.py --dynamic %s %d %s'
    work_dir = os.path.join('workdirs/%d'%(i+1))
    chost_thread = threading.Thread(
            target=run_and_print,
            args=[dynamic_hosts[i], template%(dynamic_ips[i], port_number, work_dir)]
            )
    threads.append(chost_thread)
    chost_thread.daemon = True
    chost_thread.start()
    time.sleep(10)

print client_host.cmdPrint('nc 192.168.1.0 8585 < samples/request_graph')
print client_host.cmdPrint('nc 192.168.1.0 8585 < samples/request_register')
time.sleep(10)
print client_host.cmdPrint('nc 192.168.1.1 8585 < samples/request_download')

while True:
    pass
