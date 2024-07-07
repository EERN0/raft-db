package ck.top.raft.server.proto;

import lombok.Data;

/**
 * 要票rpc响应体
 */
@Data
public class RequestVoteReply {

    // 候选者任期, 即处理rpc请求节点的任期号
    private long term;

    // 候选者获得选票时为true，否则为false
    private boolean voteGranted;

    public String toPrettyString() {
        return String.format("T%d, VoteGranted: %b", term, voteGranted);
    }
}
