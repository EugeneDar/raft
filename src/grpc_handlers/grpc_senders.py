import grpc
from google.protobuf.empty_pb2 import Empty
import queue
import threading
import time
import json

from my_logger.logger import log
from utils.cluster_info import CLUSTER_NODES
from protos.raft_pb2_grpc import RaftStub
from protos.raft_pb2 import VoteRequest, VoteResponse, LogRequest, LogResponse

from node import node

SEND_VOTE_REQUEST = 'SendVoteRequest'
SEND_VOTE_RESPONSE = 'SendVoteResponse'
SEND_LOG_REQUEST = 'SendLogRequest'
SEND_LOG_RESPONSE = 'SendLogResponse'

class RaftClient:
    def __init__(self):
        self.stubs = {node_id: self.create_stub(node_id) for node_id in CLUSTER_NODES}
        self.request_queue = queue.Queue()

        self.processing_thread = threading.Thread(target=self._process_requests, daemon=True)
        self.processing_thread.start()

    def create_stub(self, node_id):
        address = CLUSTER_NODES[node_id]
        channel = grpc.insecure_channel(address)
        return RaftStub(channel)

    def get_stub(self, node_id):
        return self.stubs[node_id]

    def _process_requests(self):
        while True:
            request, reciever_id, method = self.request_queue.get(block=True)
            try:
                if method == SEND_VOTE_REQUEST:
                    self._send_vote_request(reciever_id, *request)
                elif method == SEND_VOTE_RESPONSE:
                    self._send_vote_response(reciever_id, *request)
                elif method == SEND_LOG_REQUEST:
                    self._send_log_request(reciever_id, *request)
                elif method == SEND_LOG_RESPONSE:
                    self._send_log_response(reciever_id, *request)
            except Exception as e:
                log(f"Could not send request: {method}, to {reciever_id}")
                time.sleep(0.1)  # to wait for the node to start
                self.stubs[reciever_id] = self.create_stub(reciever_id)
                # self.request_queue.put((request, reciever_id, method))
                # raise e

    def _send_vote_request(self, reciever_id, from_node, term, log_length, log_term):
        request = VoteRequest(FromNode=from_node, Term=term, LogLength=log_length, LogTerm=log_term)
        self.get_stub(reciever_id).SendVoteRequest(request)

    def _send_vote_response(self, reciever_id, from_node, term, granted):
        request = VoteResponse(FromNode=from_node, Term=term, Granted=granted)
        self.get_stub(reciever_id).SendVoteResponse(request)

    def _send_log_request(self, reciever_id, leader_id, term, log_length, log_term, leader_commit, entries):
        request = LogRequest(LeaderId=leader_id, Term=term, LogLength=log_length, LogTerm=log_term,
                             LeaderCommit=leader_commit, Entries=[node.LogEntry(term=entry.Term, msg=entry.Msg) for entry in entries])
        self.get_stub(reciever_id).SendLogRequest(request)

    def _send_log_response(self, reciever_id, follower, term, ack, success):
        request = LogResponse(Follower=follower, Term=term, Ack=ack, Success=success)
        self.get_stub(reciever_id).SendLogResponse(request)

    def queue_vote_request(self, reciever_id, from_node, term, log_length, log_term):
        self.request_queue.put(((from_node, term, log_length, log_term), reciever_id, SEND_VOTE_REQUEST))

    def queue_vote_response(self, reciever_id, from_node, term, granted):
        self.request_queue.put(((from_node, term, granted), reciever_id, SEND_VOTE_RESPONSE))

    def queue_log_request(self, reciever_id, leader_id, term, log_length, log_term, leader_commit, entries):
        self.request_queue.put(((leader_id, term, log_length, log_term, leader_commit, entries), reciever_id, SEND_LOG_REQUEST))

    def queue_log_response(self, reciever_id, follower, term, ack, success):
        self.request_queue.put(((follower, term, ack, success), reciever_id, SEND_LOG_RESPONSE))
