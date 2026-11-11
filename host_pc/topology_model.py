import networkx as nx
import matplotlib.pyplot as plt

# 1. Model the Telecom Topology as a Graph
G = nx.Graph()

# Add Nodes (Host = Cloud, Node 1 = Edge, Node 2 = Core)
G.add_node("Cloud_Host", role="Gateway/Management", cpu=32)
G.add_node("Edge_Node1", role="Edge (mMTC)", cpu=4)
G.add_node("Core_Node2", role="Core (eMBB/URLLC)", cpu=8)

# Add Edges (Links) with weights representing network delay in ms
G.add_edge("Cloud_Host", "Edge_Node1", weight=1, bandwidth="1Gbps")
G.add_edge("Cloud_Host", "Core_Node2", weight=2, bandwidth="10Gbps")
G.add_edge("Edge_Node1", "Core_Node2", weight=5, bandwidth="5Gbps")

# 2. Calculate Path for Coordination Signaling
# In a consensus protocol, the Leader (Cloud) must send sync messages to nodes.
path_edge = nx.shortest_path(G, source="Cloud_Host", target="Edge_Node1", weight='weight')
path_core = nx.shortest_path(G, source="Cloud_Host", target="Core_Node2", weight='weight')

print("--- NetworkX Topology Analysis ---")
print(f"Path to Edge: {path_edge} | Delay: {nx.shortest_path_length(G, 'Cloud_Host', 'Edge_Node1', weight='weight')}ms")
print(f"Path to Core: {path_core} | Delay: {nx.shortest_path_length(G, 'Cloud_Host', 'Core_Node2', weight='weight')}ms")

# 3. Visualize for your report
pos = nx.spring_layout(G, seed=42)
nx.draw(G, pos, with_labels=True, node_color='lightgreen', node_size=2500, font_weight='bold')
nx.draw_networkx_edge_labels(G, pos, edge_labels=nx.get_edge_attributes(G, 'weight'))
plt.title("NetworkX Simulation: Telecom Topology for Coordination")
plt.savefig("topology_simulation.png")
print("Saved topology_simulation.png for your report.")