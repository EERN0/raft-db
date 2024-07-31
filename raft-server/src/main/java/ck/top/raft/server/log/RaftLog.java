package ck.top.raft.server.log;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * raft完整日志 = 快照日志snapshot + 尾部日志tailLog
 */
@Builder
@Data
public class RaftLog implements Serializable {

    // 1.快照日志，下标范围[1,snapLastLogIdx]
    byte[] snapshot;

    // 2.尾部日志(日志后半段)，下标范围[snapLastLogIdx+1, snapLastLogIdx + taiLog.size()-1]，含有一个虚拟头节点
    List<LogEntry> tailLog;

    // 快照日志的最后一条日志
    private int snapLastLogIdx;
    private int snapLastLogTerm;

    // 完整日志大小
    private int length;
}
