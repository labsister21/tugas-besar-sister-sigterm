package org.raft.server;

import org.raft.kvstore.rpc.ClientResponse;
import org.raft.raft.rpc.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class RaftNode {
    private static final Logger logger = Logger.getLogger(RaftNode.class.getName());

    // Node configuration
    private final String nodeId;
    private final String selfAddress;
    private final Map<String, Peer> peers = new ConcurrentHashMap<>();

    // Persistent state on all servers
    private AtomicLong currentTerm = new AtomicLong(0);
    private String votedFor = null;
    private final List<LogEntry> log = new CopyOnWriteArrayList<>();

    // Volatile state on all servers
    private volatile NodeState currentState = NodeState.FOLLOWER;
    private AtomicLong commitIndex = new AtomicLong(0);
    private AtomicLong lastApplied = new AtomicLong(0);
    private String currentLeaderId = null;

    // State Machine (Key-Value Store)
    private final ConcurrentHashMap<String, String> keyValueStore = new ConcurrentHashMap<>();

    // Internal Timer and Scheduler
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ScheduledFuture<?> electionTimeoutTask;
    private ScheduledFuture<?> heartbeatTask;
    private final Random random = new Random();
    private static final int ELECTION_TIMEOUT_MIN = 250;
    private static final int ELECTION_TIMEOUT_MAX = 400;
    private static final int HEARTBEAT_INTERVAL_MS = 100;

    // client request map
    private Map<Long, CompletableFuture<ClientResponse>> clientRequestFutures = new ConcurrentHashMap<>();

    // executor for election threads
    private final ExecutorService electionRpcExecutor = Executors.newCachedThreadPool(
            new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);

                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "election-rpc-" + nodeId + "-" + threadNumber.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            }
    );

    // executor for appendEntries threads
    private final ExecutorService appendEntriesRpcExecutor = Executors.newCachedThreadPool(
            new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);

                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "append-entries-" + nodeId + "-" + threadNumber.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            }
    );

    // constructor
    public RaftNode(String nodeId, String selfAddress, List<String> peerAddresses) {
        this.nodeId = nodeId;
        this.selfAddress = selfAddress;

        for (String peerAddress : peerAddresses) {
            String[] parts = peerAddress.split(",");
            String peerId = parts[0];
            String peerAddr = parts[1];
            if (!peerId.equals(nodeId)) {
                this.peers.put(peerId, new Peer(peerId, peerAddr));
            }
        }

        // placeholder entry
        log.add(LogEntry.newBuilder().setTerm(0).setCommand("").build());
        resetElectionTimer();
    }

    /* STATE TRANSITION */
    // method to become follower
    private synchronized void becomeFollower(long term) {
        if (term > currentTerm.get()) {
            logger.info(nodeId + " becoming Follower for term " + term + " (was " + currentState + ")");
            currentState = NodeState.FOLLOWER;
            currentTerm.set(term);
            votedFor = null;

            if (heartbeatTask != null && !heartbeatTask.isDone()) {
                heartbeatTask.cancel(false);
            }
            resetElectionTimer();
        }
    }

    // method to become candidate
    private synchronized void becomeCandidate() {
        logger.info(nodeId + " becoming Candidate for new term.");
        currentState = NodeState.CANDIDATE;
        currentTerm.incrementAndGet();
        votedFor = nodeId;
        currentLeaderId = null;
        resetElectionTimer();
        startElection();
    }

    // method to become leader
    private synchronized void becomeLeader() {
        if (currentState != NodeState.CANDIDATE) {
            logger.warning(nodeId + " tried to become leader but was not candidate. State: " + currentState);
            return;
        }
        logger.info(nodeId + " becoming Leader for term " + currentTerm.get());
        currentState = NodeState.LEADER;
        currentLeaderId = nodeId;

        if (electionTimeoutTask != null && !electionTimeoutTask.isDone()) {
            electionTimeoutTask.cancel(true);
        }

        long lastLogIdx = log.size() - 1;
        for (Peer peer : peers.values()) {
            peer.setNextIndex(lastLogIdx + 1);
            peer.setNodeMatchIndex(0);
        }
        sendHeartbeats();
        startHeartbeatTimer();
    }

    /* TIMER */
    // method to reset election timeout schedule
    private void resetElectionTimer() {
        if (electionTimeoutTask != null && !electionTimeoutTask.isDone()) {
            electionTimeoutTask.cancel(false);
        }
        if (currentState == NodeState.FOLLOWER || currentState == NodeState.CANDIDATE) {
            long timeout = ELECTION_TIMEOUT_MIN + random.nextInt(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN);
            electionTimeoutTask = scheduler.schedule(this::handleElectionTimeout, timeout, TimeUnit.MILLISECONDS);
        }
    }

    // method to handle election timeout
    private void handleElectionTimeout() {
        if (currentState == NodeState.FOLLOWER || currentState == NodeState.CANDIDATE) {
            logger.info(nodeId + " election timed out, starting new election.");
            becomeCandidate();
        }
    }

    // method to start heartbeat schedule
    private void startHeartbeatTimer() {
        if (heartbeatTask != null && !heartbeatTask.isDone()) {
            heartbeatTask.cancel(true);
        }
        heartbeatTask = scheduler.scheduleAtFixedRate(this::sendHeartbeats,
                0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /* ELECTION RELATED METHODS */
    // start election for candidates
    private void startElection() {
        final long term = currentTerm.get();
        final long lastLogIdx = log.size() - 1;
        final long lastLogTermVal = (lastLogIdx >= 0 && lastLogIdx < log.size()) ? log.get((int) lastLogIdx).getTerm() : 0;

        RequestVoteArgs request = RequestVoteArgs.newBuilder()
                .setTerm(term)
                .setCandidateId(nodeId)
                .setLastLogIndex(lastLogIdx)
                .setLastLogTerm(lastLogTermVal)
                .build();

        AtomicInteger votesReceived = new AtomicInteger(1);

        for (Peer peer : peers.values()) {
            if (currentState != NodeState.CANDIDATE || currentTerm.get() != term) {
                return;
            }

            CompletableFuture.runAsync(() -> {
                try {
                    if (currentState != NodeState.CANDIDATE || currentTerm.get() != term) {
                        return;
                    }
                    RequestVoteReply reply = peer.getBlockingStub().withDeadlineAfter(5000, TimeUnit.MILLISECONDS).requestVote(request);
                    synchronized (RaftNode.this) {
                        if (reply.getTerm() > currentTerm.get()) {
                            becomeFollower(reply.getTerm());
                            return;
                        }

                        if (currentState == NodeState.CANDIDATE && currentTerm.get() == term && reply.getTerm() == term) {
                            if (reply.getVoteGranted()) {
                                votesReceived.incrementAndGet();
                                if (votesReceived.get() >= (peers.size() + 1) / 2 + 1) {
                                    becomeLeader();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    // TODO: handle case?
                }
            }, electionRpcExecutor);
        }
    }

    // handle vote request
    public synchronized RequestVoteReply handleRequestVote(RequestVoteArgs args) {
        logger.fine(nodeId + " received RequestVote from " + args.getCandidateId() + " for term " + args.getTerm() +
                " (my term: " + currentTerm.get() + ", votedFor: " + votedFor + ")");

        boolean voteGranted = false;
        if (args.getTerm() < currentTerm.get()) {
            return RequestVoteReply.newBuilder().setTerm(currentTerm.get()).setVoteGranted(false).build();
        }

        if (args.getTerm() > currentTerm.get()) {
            becomeFollower(args.getTerm());
        } else {
            resetElectionTimer();
        }

        // check candidates
        boolean logOk = false;
        long myLastLogTerm = (!log.isEmpty()) ? log.getLast().getTerm() : 0;
        long myLastLogIndex = log.size() - 1;

        if (args.getLastLogTerm() > myLastLogTerm) {
            logOk = true;
        } else if (args.getLastLogTerm() == myLastLogTerm && args.getLastLogIndex() >= myLastLogIndex) {
            logOk = true;
        }

        if ((votedFor == null || votedFor.equals(args.getCandidateId())) && logOk) {
            votedFor = args.getCandidateId();
            voteGranted = true;
            logger.info(nodeId + " voting YES for " + args.getCandidateId() + " for term " + args.getTerm());
        } else {
            logger.info(nodeId + " voting NO for " + args.getCandidateId() + " for term " + args.getTerm() +
                    " (logOk: " + logOk + ", votedFor: " + votedFor + ")");
        }
        return RequestVoteReply.newBuilder().setTerm(currentTerm.get()).setVoteGranted(voteGranted).build();
    }

    // send hearbeat from leader
    private void sendHeartbeats() {
        synchronized (this) {
            if (currentState != NodeState.LEADER) {
                return;
            }
            logger.fine(nodeId + " (Leader) sending heartbeats for term " + currentTerm.get());
            sendAppendEntries(true);
        }
    }

    /* APPEND ENTRIES RELATED METHODS */
    // send append entries to multiple peer
    private void sendAppendEntries(boolean isHeartBeat) {
        for (Peer peer : peers.values()) {
            sendAppendEntries(peer, isHeartBeat);
        }
    }

    // send append entries to a single peer
    private void sendAppendEntries(Peer peer, boolean isHeartBeat) {
        if (currentState != NodeState.LEADER) return;
        final long nextIdx = peer.getNextIndex();
        long prevLogIdx = Math.max(0, nextIdx - 1);

        if (prevLogIdx >= log.size()) {
            peer.setNextIndex(log.size());
            prevLogIdx = Math.max(0, peer.getNextIndex() - 1);
        }

        long prevLogTerm = log.get((int) prevLogIdx).getTerm();
        AppendEntriesArgs.Builder builder = AppendEntriesArgs.newBuilder().
                setTerm(currentTerm.get()).
                setLeaderId(nodeId).
                setPrevLogIndex(prevLogIdx).
                setPrevLogTerm(prevLogTerm).
                setLeaderCommit(commitIndex.get());

        List<LogEntry> entries = new ArrayList<>();
        if (!isHeartBeat) {
            for (int i = (int) prevLogIdx + 1; i < log.size(); i++) {
                entries.add(log.get(i));
            }
        }
        builder.addAllEntries(entries);
        AppendEntriesArgs request = builder.build();

        CompletableFuture.runAsync(() -> {
            try {
                if (currentState != NodeState.LEADER) return;
                AppendEntriesReply reply = peer.getBlockingStub().withDeadlineAfter(5000, TimeUnit.MILLISECONDS).appendEntries(request);
                if (reply.getTerm() > currentTerm.get()) {
                    becomeFollower(reply.getTerm());
                    return;
                }

                if (reply.getTerm() == currentTerm.get()) {
                    if (reply.getSuccess()) {
                        long newMatchIndex = request.getPrevLogIndex() + request.getEntriesCount();
                        long newNextIndex = newMatchIndex + 1;
                        peer.setNodeMatchIndex(newMatchIndex);
                        peer.setNextIndex(newNextIndex);

                        // update commit index
                        updateCommitIndex();
                    } else if (!isHeartBeat) {
                        // send again if not heart beat
                        long newNextIdx = Math.max(1, peer.getNextIndex() - 1);
                        peer.setNextIndex(newNextIdx);
                        sendAppendEntries(peer, false);
                    }
                }

            } catch (Exception e) {
                if (!isHeartBeat) {
                    // send again if not heart beat
                    long newNextIdx = Math.max(1, peer.getNextIndex() - 1);
                    peer.setNextIndex(newNextIdx);
                    sendAppendEntries(peer, false);
                }
            }
        }, appendEntriesRpcExecutor);
    }

    // handle append entries respond
    public synchronized AppendEntriesReply handleAppendEntries(AppendEntriesArgs args) {
        if (args.getTerm() < currentTerm.get()) {
            AppendEntriesReply.newBuilder().setTerm(currentTerm.get()).setSuccess(false).setMatchIndex(0).build();
        }

        // check if current log contain the entry in log
        if (args.getPrevLogIndex() >= log.size() || args.getPrevLogIndex() < 0 || args.getPrevLogTerm() != log.get((int) args.getPrevLogIndex()).getTerm()) {
            AppendEntriesReply.newBuilder().setTerm(currentTerm.get()).setSuccess(false).setMatchIndex(0).build();
        }

        currentLeaderId = args.getLeaderId();
        if (args.getTerm() > currentTerm.get() || (currentState == NodeState.CANDIDATE && currentTerm.get() == args.getTerm())) {
            becomeFollower(args.getTerm());
        } else {
            resetElectionTimer();
        }

        // delete wrong entries
        int startIdx = 0;
        for (int i = (int)args.getPrevLogIndex() + 1; i < log.size(); i++) {
            if (log.get(i).getTerm() != args.getEntries(i - ((int)args.getPrevLogIndex() + 1)).getTerm()) {
                while (i < log.size()) {
                    log.remove(i);
                    i++;
                }
                break;
            }
            startIdx++;
        }

        // append new entries
        for (int i = 0; i < startIdx; i++) {
            log.add(args.getEntries(i));
        }

        // update commit
        if (args.getLeaderCommit() > commitIndex.get()) {
            long newCommitIndex = Math.min(args.getLeaderCommit(), log.size() - 1);
            if (newCommitIndex > commitIndex.get()) {
                commitIndex.set(newCommitIndex);
                applyCommitedEntries();
            }
        }

        return AppendEntriesReply.newBuilder()
                .setTerm(currentTerm.get())
                .setSuccess(true)
                .setMatchIndex(log.size() - 1)
                .build();
    }

    // method to update commit index for leader
    private synchronized void updateCommitIndex() {
        if (currentState != NodeState.LEADER) return;

        long newCommitIndex = commitIndex.get() + 1;
        long commIdx = commitIndex.get();
        for (; newCommitIndex < log.size(); newCommitIndex++) {
            LogEntry entry = log.get((int) newCommitIndex);
            if (entry.getTerm() != currentTerm.get()) {
                continue;
            }

            // check if majority has voted
            int aknowledgment = 1;
            for (Peer peer : peers.values()) {
                if (peer.getNodeMatchIndex() >= newCommitIndex) {
                    aknowledgment++;
                }
            }
            int totalPeer = peers.size();
            if (aknowledgment >= totalPeer / 2 + 1) {
                commIdx = newCommitIndex;
            } else {
                break;
            }
        }
        commitIndex.set(commIdx);
        if (commitIndex.get() > lastApplied.get()) {
            // apply committed entries
            applyCommitedEntries();
        }
    }

    // method to apply commited entries
    private synchronized void applyCommitedEntries() {
        while (lastApplied.get() < commitIndex.get()) {
            long applyIdx = lastApplied.incrementAndGet();
            if (applyIdx >= log.size()) {
                lastApplied.decrementAndGet(); // revert
                break;
            }
            LogEntry currLog = log.get((int) applyIdx);
            // TODO: divide between apply command and change membership
            applyCommand(currLog.getCommand());
        }
    }

    // method that apply command to the machine state
    // and return the result/error message
    private synchronized String applyCommand(String command) {
        String[] parts = command.split(" ", 3);
        String cmd = (parts.length > 0) ? parts[0] : null;
        String key = (parts.length > 1) ? parts[1] : null;
        String value = (parts.length > 2) ? parts[2] : null;

        if (cmd != null) {
            return switch (cmd) {
                case "PING" -> "PONG";
                case "GET" -> keyValueStore.getOrDefault(key, "ERROR: Key not found");
                case "SET" -> {
                    if (key != null && value != null) {
                        keyValueStore.put(key, value);
                        yield "OK";
                    }
                    yield "ERROR: Missing key or value for SET";
                }
                case "STRLEN" -> String.valueOf(keyValueStore.getOrDefault(key, "").length());
                case "DEL" -> {
                    if (key != null) {
                        String val = keyValueStore.remove(key);
                        yield val != null ? val : "";
                    }
                    yield "";
                }
                case "APPEND" -> {
                    if (key != null && value != null) {
                        keyValueStore.compute(key, (k, v) -> (v == null) ? value : v + value);
                        yield "OK";
                    }
                    yield "ERROR: Missing key or value for APPEND";
                }
                default -> "ERROR: Unknown command '" + cmd + "'";
            };
        }

        return "ERROR: Unknown command '" + command + "'";
    }

    /* CLIENT REQUEST HANDLING */
//    public ClientResponse handleClientExecute(ClientRequest request) {
//        final ClientRequest.CommandType type = request.getType();
//        final String key = request.getKey();
//        final String value = request.getValue();
//
//        CompletableFuture<ClientResponse> responseFuture = new CompletableFuture<>();
//        synchronized (this) {
//            if (currentState != NodeState.LEADER) {
//                String leaderAddr = getPeerAddress(getCurrentLeaderId());
//                ClientResponse redirectResponse = ClientResponse.newBuilder()
//                        .setSuccess(false)
//
//            }
//        }
//    }

    /* UTILITIES */
    public String getNodeId() {
        return nodeId;
    }

    public String getCurrentLeaderId() {
        return currentLeaderId;
    }

    public String getSelfAddress() {
        return selfAddress;
    }

    public NodeState getCurrentState() {
        return currentState;
    }

    private Peer getPeerById(String id) {
        if (id == null) return null;
        return peers.get(id);
    }

    private String getPeerAddress(String peerNodeId) {
        if (peerNodeId == null) return "";
        synchronized (this) {
            if (peerNodeId.equals(this.nodeId)) return this.selfAddress;
            Peer peer = peers.get(peerNodeId);
            if (peer != null) {
                return peer.getAddress();
            } else {
                return "";
            }
        }
    }

    public void shutdown() {
        logger.info(nodeId + " shutting down...");
        if (electionTimeoutTask != null) electionTimeoutTask.cancel(true);
        if (heartbeatTask != null) heartbeatTask.cancel(true);
        scheduler.shutdownNow();
        peers.values().forEach(Peer::disconnect);
        logger.info(nodeId + " scheduler and peer connections shut down.");
    }
}
