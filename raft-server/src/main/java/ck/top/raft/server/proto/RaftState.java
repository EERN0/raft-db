package ck.top.raft.server.proto;

import ck.top.raft.server.log.LogEntry;
import ck.top.raft.server.log.RaftLog;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Builder
@Data
public class RaftState implements Serializable {
    private int currentTerm;
    private String votedFor;
    private RaftLog logs;

    public RaftState() {
    }

    public RaftState(int currentTerm, String votedFor, RaftLog logs) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.logs = logs;
    }
}
