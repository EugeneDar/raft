import copy
import pytest
import time
import sys
import random
import concurrent.futures

from deploy.deploy import create_background_node
from utils.cluster_info import CLUSTER_NODES
from utils.constants import *


cluster_nodes_copy = copy.deepcopy(CLUSTER_NODES)


def set_nodes_count(nodes_count):
    CLUSTER_NODES.clear()
    for key in list(cluster_nodes_copy.keys())[:nodes_count]:
        CLUSTER_NODES[key] = cluster_nodes_copy[key]


def create_nodes(nodes_count):
    nodes = []
    stop_functions = []
    for i in range(nodes_count):
        node_id = str(i)
        node, stop_function = create_background_node(node_id)
        nodes.append(node)
        stop_functions.append(stop_function)
    return nodes, stop_functions


def stop_all_nodes(stop_functions):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(stop_function) for stop_function in stop_functions]
        concurrent.futures.wait(futures)


# @pytest.mark.parametrize("nodes_count", [
#     (2), (3), (4), (5),
#     (6), (7), (8), (9)
# ])
# def test_simple_consensus(nodes_count):
#     set_nodes_count(nodes_count)

#     nodes, stop_functions = create_nodes(nodes_count)

#     EPSILON = 0.5
#     time.sleep(3 * ELECTION_TIMEOUT_MAX + HEARTBEAT_INTERVAL + EPSILON)

#     leaders = set()
#     for node in nodes:
#         with node.lock:
#             leaders.add(node.currentLeader)

#     assert len(leaders) == 1 and list(leaders)[0] is not None

#     stop_all_nodes(stop_functions)


# @pytest.mark.parametrize("nodes_count", [
#     (3), (4), 
#     (5), (6), (7),
# ])
# def test_disable_leader(nodes_count):
#     set_nodes_count(nodes_count)

#     nodes, stop_functions = create_nodes(nodes_count)

#     EPSILON = 0.5
#     time.sleep(ELECTION_TIMEOUT_MAX + HEARTBEAT_INTERVAL + EPSILON)

#     can_disable_count = (nodes_count - 1) // 2
#     disabled = set()

#     for _ in range(can_disable_count):
#         for node in nodes:
#             with node.lock:
#                 if node.currentRole == LEADER and node.id not in disabled:
#                     stop_functions[int(node.id)]()
#                     disabled.add(node.id)
#                     break
    
#         limit_for_elections = nodes_count

#         for _ in range(limit_for_elections):
#             time.sleep(ELECTION_TIMEOUT_MAX + HEARTBEAT_INTERVAL + EPSILON)

#             leader_found = False
#             for node in nodes:
#                 with node.lock:
#                     if node.id not in disabled and node.currentRole == LEADER:
#                         leader_found = True
#                         break
#             if leader_found:
#                 break

#         assert leader_found

#         # time for leader to say everyone that he is leader
#         time.sleep(10 * HEARTBEAT_INTERVAL)

#         leaders = set()
#         for node in nodes:
#             with node.lock:
#                 if node.id not in disabled:
#                     leaders.add(node.currentLeader)

#         assert len(leaders) == 1
#         leader = list(leaders)[0]
#         assert leader is not None and leader not in disabled

#     for node in nodes:
#         if node.id in disabled:
#             continue
#         stop_functions[int(node.id)]()
            

# @pytest.mark.parametrize("nodes_count", [
#     (3), (4), 
#     (5), (6), (7),
# ])
# def test_disable_random(nodes_count):
#     set_nodes_count(nodes_count)

#     nodes, stop_functions = create_nodes(nodes_count)

#     EPSILON = 0.5
#     time.sleep(ELECTION_TIMEOUT_MAX + HEARTBEAT_INTERVAL + EPSILON)

#     can_disable_count = (nodes_count - 1) // 2
#     disabled = set()

#     for _ in range(can_disable_count):
#         while True:
#             selected_node = random.choice(nodes)
#             with selected_node.lock:
#                 if selected_node.id not in disabled:
#                     stop_functions[int(selected_node.id)]()
#                     disabled.add(selected_node.id)
#                     break
    
#         limit_for_elections = nodes_count

#         for _ in range(limit_for_elections):
#             time.sleep(ELECTION_TIMEOUT_MAX + HEARTBEAT_INTERVAL + EPSILON)

#             leader_found = False
#             for node in nodes:
#                 with node.lock:
#                     if node.id not in disabled and node.currentRole == LEADER:
#                         leader_found = True
#                         break
#             if leader_found:
#                 break

#         assert leader_found

#         # time for leader to say everyone that he is leader
#         time.sleep(10 * HEARTBEAT_INTERVAL)

#         leaders = set()
#         for node in nodes:
#             with node.lock:
#                 if node.id not in disabled:
#                     leaders.add(node.currentLeader)

#         assert len(leaders) == 1
#         leader = list(leaders)[0]
#         assert leader is not None and leader not in disabled

#     for node in nodes:
#         if node.id in disabled:
#             continue
#         stop_functions[int(node.id)]()


def write_to_wal(nodes, key, value):
    msg = {
        REQUEST_TYPE: SET,
        KEY: key,
        VALUE: value
    }
    reciever_id = '0'
    while True:
        res = nodes[int(reciever_id)].handle_client_request(msg)
        if LEADER_LOCATION in res:
            reciever_id = [node_id for node_id in CLUSTER_NODES if CLUSTER_NODES[node_id] == res[LEADER_LOCATION]][0]
        else:
            break


def read_from_wal(nodes, key):
    msg = {
        REQUEST_TYPE: GET,
        KEY: key
    }
    reciever_id = '0'
    while True:
        res = nodes[int(reciever_id)].handle_client_request(msg)
        if FOLLOWER_LOCATION in res:
            reciever_id = [node_id for node_id in CLUSTER_NODES if CLUSTER_NODES[node_id] == res[FOLLOWER_LOCATION]][0]
        else:
            return res[VALUE]


@pytest.mark.parametrize("nodes_count,ops_count", [
    (2, 3),
    (3, 5),
    (5, 9),
    (6, 3),
])
def test_write_and_read(nodes_count, ops_count):
    set_nodes_count(nodes_count)

    nodes, stop_functions = create_nodes(nodes_count)

    EPSILON = 0.5
    time.sleep(ELECTION_TIMEOUT_MAX + HEARTBEAT_INTERVAL + EPSILON)

    for step in range(ops_count):
        write_to_wal(nodes, str(step) + '_key', str(step) + '_value')
        time.sleep(5 * HEARTBEAT_INTERVAL)
        assert read_from_wal(nodes, str(step) + '_key') == str(step) + '_value'
    
    for step in range(ops_count):
        assert read_from_wal(nodes, str(step) + '_key') == str(step) + '_value'

    stop_all_nodes(stop_functions)
