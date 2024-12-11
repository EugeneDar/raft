import time
import threading

from my_logger.logger import log
from node.node import Node
from grpc_handlers.grpc_listeners import start_grpc_server
from utils.cluster_info import CLUSTER_NODES


def sleep_forever(stop_event=None):
    try:
        if stop_event:
            stop_event.wait()
            return
        while True:
            time.sleep(3)
    except KeyboardInterrupt:
        pass


def _deploy_node(node, node_id, stop_event):
    heartbeat_thread = threading.Thread(target=node.start_hearbeats, args=(stop_event,), daemon=True)
    heartbeat_thread.start()

    election_timer_thread = threading.Thread(target=node.start_election_timer, args=(stop_event,), daemon=True)
    election_timer_thread.start()

    address = CLUSTER_NODES[node_id]
    server_thread = threading.Thread(target=start_grpc_server, args=(address, node, stop_event), daemon=True)
    server_thread.start()

    log(f"Node {node_id} deployed.")

    sleep_forever(stop_event)

    heartbeat_thread.join()
    election_timer_thread.join()
    server_thread.join()
    log(f"Node {node_id} was stopped.")


def create_background_node(node_id):
    node = Node(node_id)

    stop_event = threading.Event()

    node_thread = threading.Thread(target=_deploy_node, args=(node, node_id, stop_event), daemon=True)
    node_thread.start()

    def stop_function():
        stop_event.set()
        node_thread.join()

    return node, stop_function
