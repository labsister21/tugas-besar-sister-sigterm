package org.raft.server;

import org.raft.raft.rpc.RaftServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Peer {
    private final String nodeId;
    private final String address;
    private ManagedChannel channel;
    private RaftServiceGrpc.RaftServiceBlockingStub blockingStub;
    private RaftServiceGrpc.RaftServiceStub asyncStub;

    // for leader role
    private long nextIndex;
    private long matchIndex;

    public Peer(String nodeId, String address) {
        this.nodeId = nodeId;
        this.address = address;
    }

    public void connect() {
        if (channel == null || channel.isShutdown() || channel.isTerminated()) {
            this.channel = ManagedChannelBuilder.forTarget(address)
                    .usePlaintext()
                    .build();
            this.blockingStub = RaftServiceGrpc.newBlockingStub(channel);
            this.asyncStub = RaftServiceGrpc.newStub(channel);
        }
    }

    public void disconnect() {
        if (channel != null) {
            channel.shutdown();
            channel = null;
            blockingStub = null;
            asyncStub = null;
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getAddress() {
        return address;
    }

    public RaftServiceGrpc.RaftServiceBlockingStub getBlockingStub() {
        if (blockingStub == null) connect();
        return blockingStub;
    }

    public RaftServiceGrpc.RaftServiceStub getAsyncStub() {
        if (asyncStub == null) connect();
        return asyncStub;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getNodeMatchIndex() {
        return matchIndex;
    }

    public void setNodeMatchIndex(long nodeMatchIndex) {
        this.matchIndex = nodeMatchIndex;
    }
}
