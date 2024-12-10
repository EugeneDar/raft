import copy
import pytest
import time
import sys
import concurrent.futures

from deploy.deploy import create_background_node
from utils.cluster_info import CLUSTER_NODES
from utils.constants import *


cluster_nodes_copy = copy.deepcopy(CLUSTER_NODES)


def set_nodes_count(nodes_count):
    CLUSTER_NODES.clear()
    for key in list(cluster_nodes_copy.keys())[:nodes_count]:
        CLUSTER_NODES[key] = cluster_nodes_copy[key]


def stop_all_nodes(stop_functions):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(stop_function) for stop_function in stop_functions]
        concurrent.futures.wait(futures)


@pytest.mark.parametrize("amount", [
    (2), (3), (4), (5),
    (6), (7), (8), (9)
])
def test_simple(amount):
    print(f"TEST: test_add({amount})")
    set_nodes_count(amount)

    nodes = []
    stop_functions = []
    for i in range(amount):
        node_id = str(i)
        node, stop_function = create_background_node(node_id)
        nodes.append(node)
        stop_functions.append(stop_function)

    EPSILON = 0.5
    time.sleep(ELECTION_TIMEOUT_MAX + HEARTBEAT_INTERVAL + EPSILON)

    leaders = set()
    for node in nodes:
        with node.lock:
            print(f"Node {node.id} is {node.currentRole}, leader is {node.currentLeader}")
            leaders.add(node.currentLeader)
    assert len(leaders) == 1

    stop_all_nodes(stop_functions)
