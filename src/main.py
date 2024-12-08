import threading
import time

from grpc_handlers.grpc_listeners import start_grpc_server
from utils.cluster_info import CLUSTER_NODES
from node.node import Node

def sleep_forever():
    while True:
        time.sleep(5)


def deploy_node(node_id):
    print(f"Deploying node {node_id}")
    node = Node(node_id)

    heartbeat_thread = threading.Thread(target=node.start_hearbeats, daemon=True)
    heartbeat_thread.start()

    election_timer_thread = threading.Thread(target=node.start_election_timer, daemon=True)
    election_timer_thread.start()

    address = CLUSTER_NODES[node_id]
    server_thread = threading.Thread(target=start_grpc_server, args=(address, node), daemon=True)
    server_thread.start()

    print(f"Node {node_id} deployed.")
    sleep_forever()


def main():
    print("Starting Raft cluster...")
    for node_id in CLUSTER_NODES:
        node_thread = threading.Thread(target=deploy_node, args=node_id, daemon=True)
        node_thread.start()

    sleep_forever()


if __name__ == "__main__":
    main()
