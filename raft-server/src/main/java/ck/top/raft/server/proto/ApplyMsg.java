package ck.top.raft.server.proto;

public class ApplyMsg {
    private boolean commandValid;
    private Command command;
    private int commandIndex;
}
