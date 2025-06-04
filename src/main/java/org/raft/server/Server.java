package org.raft.server;

import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Server {
    private static final Logger logger = Logger.getLogger(Server.class.getName());
    private io.grpc.Server grpcServer;
    private RaftNode raftNode;

    public void start(String nodeId, String selfAddress, List<String> peerAddresses) throws IOException {
        int port = Integer.parseInt(selfAddress.split(":")[1]);

        raftNode = new RaftNode(nodeId, selfAddress, peerAddresses);

        grpcServer = ServerBuilder.forPort(port)
                .addService(new RaftService(raftNode))
                .addService(new KVStoreService(raftNode))
                .build()
                .start();

        logger.info("Server " + nodeId + " started, listening on " + selfAddress);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            Server.this.stop();
            System.err.println("*** server shut down");
        }));
    }

    public void stop() {
        if (raftNode != null) {
            raftNode.shutdown();
        }
        if (grpcServer != null) {
            try {
                grpcServer.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: Server <nodeId> <selfHost:port> [peer1Id,peer1Host:port peer2Id,peer2Host:port ...]");
            System.exit(1);
        }

        String nodeId = args[0];
        String selfAddr = args[1];
        List<String> peerAddrs = Arrays.asList(args).subList(2, args.length);

        final Server server = new Server();
        server.start(nodeId, selfAddr, peerAddrs);
        server.blockUntilShutdown();
    }
}
