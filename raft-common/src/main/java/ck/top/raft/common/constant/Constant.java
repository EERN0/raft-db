package ck.top.raft.common.constant;

public class Constant {
    // 选举超时时间的上下界
    public static final long ELECTION_TIMEOUT_MIN = 250;
    public static final long ELECTION_TIMEOUT_MAX = 400;

    // 发送心跳、日志同步的时间间隔，小于选举超时下界
    public static final long REPLICATE_INTERVAL = 100;
}
