package ck.top.raft.server.proto;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogEntry implements Serializable {

    // 日志条目的任期
    private int term = -1;
    // 日志条目索引
    private int index = 0;

    // 具体的命令
    private Object command;
    // 有效命令将被执行
    private boolean isValid;
}
