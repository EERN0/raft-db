package ck.top.raft.server.proto;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * leader才会发送心跳、日志复制请求
 */
@Data
public class AppendEntriesArgs implements Serializable {

    // leader任期
    private int term;
    private String leaderId;

    /**
     * leader日志中的前一个条目的索引，用于检查是否与follower冲突
     * 对应的是日志索引，使用前要转换成数组对应的索引
     */
    private int preLogIndex = 0;

    // leader前一条日志的任期，用于检查term是否一致
    private int preLogTerm = -1;

    // 需要复制到follower日志中的新日志。可以是多个日志条目，也可以是空的（心跳）
    private List<LogEntry> entries = new ArrayList<>();

    // leader已提交的日志索引，通过rpc发给每个follower
    private int leaderCommitIndex;
}