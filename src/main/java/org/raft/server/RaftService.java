package org.raft.server;

import io.grpc.stub.StreamObserver;
import org.raft.raft.rpc.*;


public class RaftService extends RaftServiceGrpc.RaftServiceImplBase {
    private final RaftNode raftNode;

    public RaftService(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void requestVote(RequestVoteArgs request, StreamObserver<RequestVoteReply> responseObserver) {
        RequestVoteReply reply = raftNode.handleRequestVote(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void appendEntries(AppendEntriesArgs request, StreamObserver<AppendEntriesReply> responseObserver) {
        AppendEntriesReply reply = raftNode.handleAppendEntries(request);
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
