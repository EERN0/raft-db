package ck.top.raft.server.proto;

import lombok.Data;

import java.io.Serializable;

@Data
public class ClientRequest implements Serializable {

    public static final int INSERT = 1;
    public static final int SEARCH = 2;
    public static final int GET = 3;
    public static final int DELETE = 4;
    public static final int DISPLAY = 5;

    // 请求类型
    private int cmd;

    // 键值对
    private String key;
    private String value;


}
