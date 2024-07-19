package ck.top.raft.server.proto;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class ApplyMsg {
    private boolean commandValid;
    private Command command;
    private int commandIndex;
}
