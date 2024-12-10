import time
import threading

from node.node import Node
from grpc_handlers.grpc_listeners import start_grpc_server
from utils.cluster_info import CLUSTER_NODES


def sleep_forever(node_id=None, stop_event=None):
    try:
        while True:
            time.sleep(3)
            if stop_event and stop_event.is_set():
                print(f"Node {node_id} was stopped by stop_event.")
                break
    except KeyboardInterrupt:
        if node_id:
            print(f"Node {node_id} was stopped by KeyboardInterrupt.")


def _deploy_node(node, node_id, stop_event):
    print(f"Deploying node {node_id}")
    heartbeat_thread = threading.Thread(target=node.start_hearbeats, daemon=True)
    heartbeat_thread.start()

    election_timer_thread = threading.Thread(target=node.start_election_timer, daemon=True)
    election_timer_thread.start()

    address = CLUSTER_NODES[node_id]
    server_thread = threading.Thread(target=start_grpc_server, args=(address, node), daemon=True)
    server_thread.start()

    print(f"Node {node_id} deployed.")
    sleep_forever(node_id, stop_event)


def create_background_node(node_id):
    node = Node(node_id)

    stop_event = threading.Event()

    node_thread = threading.Thread(target=_deploy_node, args=(node, node_id, stop_event), daemon=True)
    node_thread.start()

    def stop_function():
        stop_event.set()
        node_thread.join()

    return node, stop_function
