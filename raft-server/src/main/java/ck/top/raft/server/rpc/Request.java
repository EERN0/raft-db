package ck.top.raft.server.rpc;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Builder
@Data
public class Request implements Serializable {

    // 要票请求
    public static final int REQUEST_VOTE = 0;
    // 心跳、日志同步请求
    public static final int APPEND_ENTRIES = 1;
    // 客户端请求
    public static final int CLIENT_REQUEST = 2;

    // 请求类型
    private int cmd = -1;

    // 请求参数
    private Object obj;

    // peer地址
    private String url;

    public Request(int cmd, Object obj, String url) {
        this.cmd = cmd;
        this.obj = obj;
        this.url = url;
    }
}
