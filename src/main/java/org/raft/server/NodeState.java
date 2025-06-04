package org.raft.server;

public enum NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
