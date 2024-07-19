package ck.top.raft.server.proto;

import ck.top.raft.server.log.LogEntry;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Builder
@Data
public class RaftState implements Serializable {
    private int currentTerm;
    private String votedFor;
    private List<LogEntry> logs;

    public RaftState(int currentTerm, String votedFor, List<LogEntry> logs) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.logs = logs;
    }
}
