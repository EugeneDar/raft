import grpc
from concurrent.futures import ThreadPoolExecutor
from google.protobuf.empty_pb2 import Empty

from protos.raft_pb2_grpc import add_RaftServicer_to_server, RaftServicer

from node import node


class RaftService(RaftServicer):
    def __init__(self, node):
        self.node = node

    def SendVoteRequest(self, request, context):
        self.node.handle_vote_request(request.FromNode, request.Term, request.LogLength, request.LogTerm)
        return Empty()

    def SendVoteResponse(self, request, context):
        self.node.handle_vote_response(request.FromNode, request.Term, request.Granted)
        return Empty()

    def SendLogRequest(self, request, context):
        entries = [node.LogEntry(term=entry.Term, msg=entry.Msg) for entry in request.Entries]
        self.node.handle_log_request(request.LeaderId, request.Term, request.LogLength, request.LogTerm, request.LeaderCommit, entries)
        return Empty()

    def SendLogResponse(self, request, context):
        self.node.handle_log_response(request.Follower, request.Term, request.Ack, request.Success)
        return Empty()


def start_grpc_server(address, node):
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    add_RaftServicer_to_server(RaftService(node), server)
    server.add_insecure_port(address)
    print(f"gRPC server started on {address}.")
    server.start()
    server.wait_for_termination()