package org.raft.server;

import io.grpc.stub.StreamObserver;
import org.raft.kvstore.rpc.*;

public class KVStoreService extends KVStoreServiceGrpc.KVStoreServiceImplBase {
    private final RaftNode raftNode;

    public KVStoreService(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void executeCommand(ClientRequest request, StreamObserver<ClientResponse> responseObserver) {
        // TODO: implement handle client execute
//        ClientResponse response = raftNode.handleClientExecute(request);
//        responseObserver.onNext(response);
//        responseObserver.onCompleted();
    }

    @Override
    public void requestLog(RequestLogArgs request, StreamObserver<RequestLogReply> responseObserver) {
        // TODO: implement handle request log
//        RequestLogReply response = raftNode.handleRequestLog();
//        responseObserver.onNext(response);
//        responseObserver.onCompleted();
    }
}
