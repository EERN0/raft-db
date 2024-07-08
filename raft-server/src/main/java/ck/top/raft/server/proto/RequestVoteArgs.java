package ck.top.raft.server.proto;

import lombok.Data;

import java.io.Serializable;

/**
 * 候选者candidate发起要票rpc
 * 要票rpc的请求参数
 */
@Data
public class RequestVoteArgs implements Serializable {
    // 候选者任期
    private long term;

    // 其它raft peer节点id，格式为ip:port
    private String peerId;
    // 候选者id, 格式为ip:port
    private String candidateId;

    // 最后一条日志索引
    private long lastLogIndex;
    // 最后一条日志任期
    private long lastLogTerm;

    public String toPrettyString() {
        return String.format("Candidate-%s, T%d, LastLogIdx: [%d]T%d", candidateId, term, lastLogIndex, lastLogTerm);
    }
}