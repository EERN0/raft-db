package ck.top.raft.server.proto;

import lombok.Data;

import java.io.Serializable;

@Data
public class AppendEntriesReply implements Serializable {

    // 接收方任期
    private int term;

    // 日志复制成功-true，日志冲突-false
    private boolean succeeded;

    /**
     * 复制不成功则修改该值
     * 告知 leader 最靠后的冲突日志的索引
     */
    private int conflictLogIndex;
    // 冲突日志对应的任期
    private int conflictLogTerm;
}