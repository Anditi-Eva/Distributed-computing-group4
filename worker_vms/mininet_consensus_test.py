from mininet.net import Mininet
from mininet.node import Controller
from mininet.cli import CLI
from mininet.log import setLogLevel
import time

def run_mininet():
    setLogLevel('info')
    net = Mininet(controller=Controller)
    net.addController('c0')
    
    # Create network topology
    s1 = net.addSwitch('s1')
    h1 = net.addHost('h1', ip='10.0.0.1') # Slice User
    h2 = net.addHost('h2', ip='10.0.0.2') # Slice Server
    
    net.addLink(h1, s1)
    net.addLink(s1, h2)
    net.start()
    
    print("\n*** Measuring Signaling Delay in Mininet ***")
    h1 = net.get('h1')
    # Measure latency between slice hosts
    delay_output = h1.cmd('ping -c 5 10.0.0.2')
    print(delay_output)
    
    print("\n*** Testing Consistency (Packet Loss) ***")
    loss = net.pingAll()
    print(f"Packet Loss: {loss}%")
    
    net.stop()

if __name__ == '__main__':
    run_mininet()