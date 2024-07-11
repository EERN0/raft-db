package ck.top.raft.server.proto;

import lombok.Data;

import java.io.Serializable;

/**
 * 要票rpc响应体
 */
@Data
public class RequestVoteReply implements Serializable {

    // 处理要票rpc请求的节点的任期，非candidate的任期
    private int term;

    // 节点投票给候选者时为true，否则为false
    private boolean voteGranted;

    public String toPrettyString() {
        return String.format("T%d, VoteGranted: %b", term, voteGranted);
    }
}
