from mininet.net import Mininet
from mininet.node import OVSController
from mininet.log import setLogLevel
import time

def run_mininet():
    setLogLevel('info')
    
    # Use OVSController - it is built-in and runs locally on the switch
    net = Mininet(controller=OVSController)
    
    print("*** Creating network")
    net.addController('c0')
    
    # Create network topology
    s1 = net.addSwitch('s1')
    h1 = net.addHost('h1', ip='10.0.0.1') # Slice User
    h2 = net.addHost('h2', ip='10.0.0.2') # Slice Server
    
    net.addLink(h1, s1)
    net.addLink(s1, h2)
    
    net.start()
    
    # --- CRITICAL FIX: WAIT FOR NETWORK TO CONVERGE ---
    print("\n*** Allowing 3 seconds for controller to install flow rules...")
    time.sleep(3) 
    
    print("\n*** Measuring Signaling Delay in Mininet ***")
    h1 = net.get('h1')
    # Measure latency between slice hosts
    delay_output = h1.cmd('ping -c 3 10.0.0.2')
    print(delay_output)
    
    print("\n*** Testing Consistency (Packet Loss) ***")
    loss = net.pingAll()
    print(f"Packet Loss Percentage: {loss}%")
    
    print("\n*** Stopping Network")
    net.stop()

if __name__ == '__main__':
    run_mininet()