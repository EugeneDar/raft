import copy
import pytest
import time
import logging
import sys

from deploy.deploy import create_background_node
from utils.cluster_info import CLUSTER_NODES
from utils.constants import *


logging.basicConfig(level=logging.DEBUG)

cluster_nodes_copy = copy.deepcopy(CLUSTER_NODES)


def set_nodes_count(nodes_count):
    CLUSTER_NODES.clear()
    for key in list(cluster_nodes_copy.keys())[:nodes_count]:
        CLUSTER_NODES[key] = cluster_nodes_copy[key]


@pytest.mark.parametrize("amount", [
    (2), (3), (4), (5),
    (6), (7), (8), (9)
])
def test_add(amount):
    print(f"TEST: test_add({amount})")
    set_nodes_count(amount)
    
    nodes = []
    stop_functions = []
    for i in range(amount):
        node_id = str(i)
        node, stop_function = create_background_node(node_id)
        nodes.append(node)
        stop_functions.append(stop_function)

    time.sleep(ELECTION_TIMEOUT_MAX + 1)
    for stop_function in stop_functions:
        stop_function()
