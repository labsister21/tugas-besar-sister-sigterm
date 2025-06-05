package org.raft.client;

import org.raft.kvstore.rpc.*;
import org.raft.raft.rpc.LogEntry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());
    private ManagedChannel channel;
    private KVStoreServiceGrpc.KVStoreServiceBlockingStub blockingStub;
    private String currentServerAddress;
    private static final int MAX_REDIRECTS = 5;

    public Client(String initialServerAddress) {
        this.currentServerAddress = initialServerAddress;
        connect(initialServerAddress);
    }

    private void connect(String serverAddress) {
        shutdown();
        logger.info("Trying to connect to servery: " + serverAddress);
        try {
            this.channel = ManagedChannelBuilder.forTarget(serverAddress)
                    .usePlaintext()
                    .build();
            this.blockingStub = KVStoreServiceGrpc.newBlockingStub(channel);
            this.currentServerAddress = serverAddress;
            logger.info("Successfully connected to: " + serverAddress);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to connect to " + serverAddress + ": " + e.getMessage());
            this.channel = null;
            this.blockingStub = null;
        }
    }

    public void shutdown() {
        if (channel != null && !channel.isShutdown()) {
            try {
                logger.info("Closing connection to: " + currentServerAddress);
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "Failed to close connection properly.", e);
                Thread.currentThread().interrupt();
            }
        }
        channel = null;
        blockingStub = null;
    }

    public void executeCommand(String commandString) {
        if (commandString == null || commandString.trim().isEmpty()) {
            logger.warning("Command must not be empty.");
            return;
        }

        if (commandString.startsWith("connect")) {
            String[] parts = commandString.split(" ", 2);
            if (parts.length == 2 && !parts[1].trim().isEmpty()) {
                connect(parts[1].trim());
            } else {
                logger.warning("Usage: connect <host:port>");
            }
            return;
        }
        if (commandString.equalsIgnoreCase("exit") || commandString.equalsIgnoreCase("quit")) {
            System.out.println("Exit from client.");
            shutdown();
            System.exit(0);
        }


        int redirects = 0;
        String rawCommand = commandString;

        while (redirects < MAX_REDIRECTS) {
            if (blockingStub == null) {
                logger.severe("Not connected to any server.\nTry to run 'connect <host:port>' first.");
                System.out.println("ERROR: Not connected to any server.");
                return;
            }

            try {
                if (rawCommand.equalsIgnoreCase("REQUEST_LOG")) {
                    RequestLogArgs logRequest = RequestLogArgs.newBuilder().build();
                    logger.info("Sending REQUEST_LOG to " + currentServerAddress);
                    RequestLogReply logReply = blockingStub.requestLog(logRequest);

                    if (logReply.getSuccess()) {
                        System.out.println("Log from leader (" + logReply.getLeaderAddress() + "):");
                        if (logReply.getLogsCount() == 0) {
                            System.out.println("(Empty log)");
                        } else {
                            for (int i = 0; i < logReply.getLogsCount(); i++) {
                                LogEntry entry = logReply.getLogs(i);
                                System.out.print("  Index " + i + ": Term=" + entry.getTerm());
                                System.out.print("  Type=" + entry.getType());
                                if (entry.getKey() != null && !entry.getKey().isEmpty()) {
                                    System.out.print("  Key=" + entry.getKey());
                                }
                                if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                                    System.out.print("  Value=" + entry.getValue());
                                }
                                if (!entry.getOldConfList().isEmpty()) {
                                    System.out.print("  OldConfList=" + entry.getOldConfList());
                                }
                                if (!entry.getNewConfList().isEmpty()) {
                                    System.out.print("  NewConfList=" + entry.getNewConfList());
                                }
                                System.out.println();
                            }
                        }
                        return;
                    } else {
                        if (!logReply.getLeaderAddress().isEmpty()) {
                            logger.info("Not leader.\nLeader Address: " + logReply.getLeaderAddress() + ". Mencoba redirect...");
                            System.out.println("REDIRECT: This node is not a leader. Trying to redirect to " + logReply.getLeaderAddress());
                            connect(logReply.getLeaderAddress());
                            redirects++;
                        } else {
                            logger.warning("Log request failed.");
                            System.out.println("ERROR: Failed to get log.\nThere's no information about the leader address.");
                            return;
                        }
                    }
                } else if (rawCommand.startsWith("ADD_MEMBER")) {
                    String[] res = commandString.split(" ");
                    String nodeId = res[1];
                    String nodeAddress = res[2];
                    MemberChangeArgs request = MemberChangeArgs.newBuilder()
                            .setType(MemberChangeArgs.ChangeType.ADD)
                            .setNodeId(nodeId)
                            .setNodeAddress(nodeAddress)
                            .build();
                    try {
                        MemberChangeReply response = blockingStub.changeMembership(request);
                        if (response.getSuccess()) {
                            System.out.println("Member added successfully");
                        } else {
                            System.out.println("ERROR: " + response.getErrorMessage());
                        }
                    } catch (StatusRuntimeException e) {
                        System.out.println("ERROR: Failed to add member: " + e.getStatus());
                    }
                    redirects = MAX_REDIRECTS;
                } else if (rawCommand.startsWith("REMOVE_MEMBER")) {
                    String[] res = commandString.split(" ");
                    String nodeId = res[1];
                    MemberChangeArgs request = MemberChangeArgs.newBuilder()
                            .setType(MemberChangeArgs.ChangeType.REMOVE)
                            .setNodeId(nodeId)
                            .build();
                    try {
                        MemberChangeReply response = blockingStub.changeMembership(request);
                        if (response.getSuccess()) {
                            System.out.println("Member added successfully");
                        } else {
                            System.out.println("ERROR: " + response.getErrorMessage());
                        }
                    } catch (StatusRuntimeException e) {
                        System.out.println("ERROR: Failed to add member: " + e.getStatus());
                    }
                    redirects = MAX_REDIRECTS;
                } else {
                    String[] res = commandString.split(" ");
                    String cmd = res[0];
                    String key = res.length > 1 ? res[1] : null;
                    String value = res.length > 2 ? res[2] : null;

                    ClientRequest.Builder builder = ClientRequest.newBuilder().setType(ClientRequest.CommandType.valueOf(cmd));
                    if (key != null) {
                        builder.setKey(key);
                    }
                    if (value != null) {
                        builder.setValue(value);
                    }
                    ClientRequest request = builder.build();
                    logger.info("Sending command '" + rawCommand + "' to " + currentServerAddress);
                    ClientResponse response = blockingStub.executeCommand(request);

                    if (response.getSuccess()) {
                        System.out.println(response.getResult());
                        return;
                    } else {
                        if (!response.getLeaderAddress().isEmpty()) {
                            logger.info("Not a leader.\nLeader address: " + response.getLeaderAddress() + ".\nTrying to redirect...");
                            System.out.println("REDIRECT: Not a leader. Trying to redirect to " + response.getLeaderAddress());
                            connect(response.getLeaderAddress());
                            redirects++;
                        } else {
                            logger.warning("Command failed: " + response.getResult());
                            System.out.println("ERROR: " + (!response.getResult().isEmpty() ? response.getResult() : "Command failed."));
                            return;
                        }
                    }
                }
            } catch (StatusRuntimeException e) {
                logger.log(Level.SEVERE, "RPC failed to " + currentServerAddress + ": " + e.getStatus(), e);
                System.out.println("ERROR: Failed to communicate to " + currentServerAddress + " (" + e.getStatus().getCode() + ").\nTry another server.");
                return;
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error when sending command to " + currentServerAddress + ": " + e.getMessage(), e);
                System.out.println("ERROR: " + e.getMessage());
                return;
            }
        }

        logger.warning("Maximum redirect reached");
        System.out.println("ERROR: Failed to redirect after " + MAX_REDIRECTS + " tries.");
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: Client <initial_server_address:port> [command...]");
            System.exit(1);
        }

        String initialServerAddress = args[0];
        Client client = new Client(initialServerAddress);

        if (args.length > 1) {
            List<String> commandParts = Arrays.asList(args).subList(1, args.length);
            String commandString = String.join(" ", commandParts);
            client.executeCommand(commandString);
            client.shutdown();
        } else {
            System.out.println("Enter 'exit' or 'quit' to exit the client.");
            System.out.println("Enter 'connect <host:port>' to change the server connectino");
            Scanner scanner = new Scanner(System.in);
            while (true) {
                if (client.currentServerAddress != null) {
                    System.out.print("[" + client.currentServerAddress + "] > ");
                } else {
                    System.out.print("[disconnected] > ");
                }
                String line = scanner.nextLine();
                if (line == null) break;

                String trimmedLine = line.trim();
                if (trimmedLine.isEmpty()) continue;

                client.executeCommand(trimmedLine);
            }
            scanner.close();
            client.shutdown();
        }
    }
}
