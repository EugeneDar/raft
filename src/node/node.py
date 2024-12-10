import random
import threading
import time

from utils.constants import *
from utils.cluster_info import CLUSTER_NODES
from storage.storage import Storage
from grpc_handlers.grpc_senders import RaftClient


class LogEntry:
    def __init__(self, term, msg):
        self.term = term
        self.msg = msg


class Node:
    # on initialization
    def __init__(self, id):
        self.id = id
        self.lock = threading.Lock()
        self.storage = Storage()
        self.shouldNotStartNewElection = False
        self.grpcClient = RaftClient()

        self.currentTerm = 0
        self.votedFor = None
        self.log = []
        self.commitLength = 0
        self.currentRole = FOLLOWER
        self.currentLeader = None
        self.votesReceived = set()
        self.sentLength = {}
        self.ackedLength = {}

    # on recovery from crash
    def recovery_from_crash(self):
        self.currentRole = FOLLOWER
        self.currentLeader = None
        self.votesReceived = set()
        self.sentLength = {}
        self.ackedLength = {}

    # нужно руками вызывать в фоне
    def start_election_timer(self, stop_event):
        while not stop_event.is_set():
            timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
            stop_event.wait(timeout)
            if stop_event.is_set():
                break
            with self.lock:
                if self.currentRole == LEADER:
                    self.shouldNotStartNewElection = True
                if self.shouldNotStartNewElection:
                    self.shouldNotStartNewElection = False
                    continue
                print(f"[Node {self.id}]: starting election")
                self.start_election()

    # on node nodeId suspects leader has failed, or on election timeout
    def start_election(self):
        self.currentTerm += 1
        self.currentRole = CANDIDATE
        self.votedFor = self.id
        self.votesReceived = {self.id}
        lastTerm = 0
        if len(self.log) > 0:
            lastTerm = self.log[-1].term
        for node_id in CLUSTER_NODES:
            if node_id == self.id:
                continue
            print(f"[Node {self.id}]: sending vote request to {node_id}")
            self.grpcClient.queue_vote_request(node_id, self.id, self.currentTerm, len(self.log), lastTerm)

    def handle_vote_request(self, fromNode, term, logLength, logTerm):
        print(f"[Node {self.id}]: received vote request from {fromNode}")
        with self.lock:
            myLogTerm = self.log[-1].term if len(self.log) > 0 else 0
            logOk = (logTerm > myLogTerm) or (logTerm == myLogTerm and logLength >= len(self.log))
            termOk = (term > self.currentTerm) or (term == self.currentTerm and self.votedFor in {None, fromNode})

            if logOk and termOk:
                self.currentTerm = term
                self.currentRole = FOLLOWER
                self.votedFor = fromNode
                print(f"[Node {self.id}]: voted for {fromNode}")
                self.grpcClient.queue_vote_response(fromNode, self.id, self.currentTerm, True)
                self.shouldNotStartNewElection = True
            else:
                debug_reasons = f"#{fromNode} term: {term}, my term: {self.currentTerm}"
                print(f"[Node {self.id}]: rejected vote for {fromNode}, {debug_reasons}")
                self.grpcClient.queue_vote_response(fromNode, self.id, self.currentTerm, False)
    
    def handle_vote_response(self, fromNode, term, granted):
        with self.lock:
            if self.currentRole == CANDIDATE and term == self.currentTerm and granted:
                self.votesReceived.add(fromNode)
                if len(self.votesReceived) > len(CLUSTER_NODES) // 2:
                    print(f"[Node {self.id}]: became leader!!!")
                    self.currentRole = LEADER
                    self.currentLeader = self.id
                    for follower in CLUSTER_NODES:
                        if follower == self.id:
                            continue
                        self.sentLength[follower] = len(self.log)
                        self.ackedLength[follower] = 0
                        self.replicate_log(follower)
            elif term > self.currentTerm:
                self.currentTerm = term
                self.currentRole = FOLLOWER
                self.votedFor = None
                self.shouldNotStartNewElection = True

    # on request to broadcast msg at node nodeId
    def handle_client_request(self, msg):
        print(f"[Node {self.id}]: received client request")
        with self.lock:
            if self.currentRole == LEADER:
                self.log.append(LogEntry(term=self.currentTerm, msg=msg))
                self.ackedLength[self.id] = len(self.log)
                for follower in CLUSTER_NODES:
                    if follower == self.id:
                        continue
                    self.replicate_log(follower)
            else:
                # TODO: redirect to leader
                # self.send_msg(self.currentLeader, f"ClientRequest, {self.id}, {msg}")
                pass
    
    # нужно руками вызывать в фоне
    def start_hearbeats(self, stop_event):
        while not stop_event.is_set():
            stop_event.wait(HEARTBEAT_INTERVAL)
            if stop_event.is_set():
                break
            with self.lock:
                if self.currentRole != LEADER:
                    continue
                for follower in CLUSTER_NODES:
                    if follower == self.id:
                        continue
                    self.replicate_log(follower)

    def replicate_log(self, followerId):
        # print(f"[Node {self.id}]: sending log request to {followerId}")
        i = self.sentLength[followerId]
        entries = self.log[i:]
        prevLogTerm = 0
        if i > 0:
            prevLogTerm = self.log[i - 1].term
        self.grpcClient.queue_log_request(followerId, self.id, self.currentTerm, i, prevLogTerm, self.commitLength, entries)

    def handle_log_request(self, leaderId, term, logLength, logTerm, leaderCommit, entries):
        # print(f"[Node {self.id}]: received log request from {leaderId}")
        with self.lock:
            if term > self.currentTerm:
                print(f"[Node {self.id}]: update leader to {leaderId}")
                self.currentTerm = term
                self.votedFor = None
                self.currentRole = FOLLOWER
                self.currentLeader = leaderId
            if term == self.currentTerm:
            # todo think about this
            # if term == self.currentTerm and self.currentRole == CANDIDATE:
                self.currentRole = FOLLOWER
                self.currentLeader = leaderId
            logOk = (len(self.log) >= logLength) and (logLength == 0 or logTerm == self.log[logLength - 1].term)
            if term == self.currentTerm and logOk:
                self.append_entries(logLength, leaderCommit, entries)
                ack = logLength + len(entries)
                self.grpcClient.queue_log_response(leaderId, self.id, self.currentTerm, ack, True)
            else:
                self.grpcClient.queue_log_response(leaderId, self.id, self.currentTerm, 0, False)
            self.shouldNotStartNewElection = True

    def append_entries(self, logLength, leaderCommit, entries):
        if len(entries) > 0 and len(self.log) > logLength:
            if self.log[logLength].term != entries[0].term:
                self.log = self.log[:logLength]
        if logLength + len(entries) > len(self.log):
            for i in range(len(self.log) - logLength, len(entries)):
                self.log.append(entries[i])
        if leaderCommit > self.commitLength:
            for i in range(self.commitLength, leaderCommit):
                self.deliver_log(i)
            self.commitLength = leaderCommit

    def deliver_log(self, index):
        msg = self.log[index].msg
        self.storage.handle_log_delivery(msg)

    def handle_log_response(self, follower, term, ack, success):
        with self.lock:
            if term == self.currentTerm and self.currentRole == LEADER:
                if success == True and ack >= self.ackedLength[follower]:
                    self.sentLength[follower] = ack
                    self.ackedLength[follower] = ack
                    self.commit_log_entries()
                elif self.sentLength[follower] > 0:
                    self.sentLength[follower] -= 1
                    self.replicate_log(follower)
            elif term > self.currentTerm:
                self.currentTerm = term
                self.currentRole = FOLLOWER
                self.votedFor = None
                self.currentLeader = None

    def commit_log_entries(self):
        ackedLengths = sorted(self.ackedLength.values())
        max_ready = ackedLengths[(len(CLUSTER_NODES) - 1) // 2]
        if max_ready > self.commitLength and self.log[max_ready - 1].term == self.currentTerm:
            for i in range(self.commitLength, max_ready):
                self.deliver_log(i)
            self.commitLength = max_ready
