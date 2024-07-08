package ck.top.raft.server.proto;

import lombok.Data;

import java.io.Serializable;

@Data
public class AppendEntriesReply implements Serializable {

    // 接收方任期
    private long term;

    // 是否复制成功
    private boolean succeeded;

    /**
     * 复制不成功则修改该值
     * 告知 leader 最靠后的冲突日志的索引
     */
    private int conflictIndex;
    // 冲突日志对应的任期
    private int conflictTerm;
}