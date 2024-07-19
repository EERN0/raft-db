package ck.top.raft.server.proto;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class Command implements Serializable {

    public static final int INSERT = 1;
    public static final int SEARCH = 2;
    public static final int GET = 3;
    public static final int DELETE = 4;
    public static final int DISPLAY = 5;

    int cmd;
    String key;
    String value;
}
