package ck.top.raft.server.proto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Builder
@Data
public class RaftState {
    private int currentTerm;
    private String votedFor;
    private List<LogEntry> log;

    public RaftState(int currentTerm, String votedFor, List<LogEntry> log) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log = log;
    }
}
