package ck.top.raft.server.proto;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class LogEntry implements Serializable {

    // 日志条目的任期
    private int term = 0;
    // 日志条目索引
    private int index = 0;

    // 具体的命令
    private Command command;
    // 有效命令将被执行
    private boolean isValid;

    public LogEntry(int index, int term) {
        this.index = index;
        this.term = term;
    }
}
