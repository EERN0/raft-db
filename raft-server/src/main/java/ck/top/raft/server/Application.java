package ck.top.raft.server;

import ck.top.raft.server.proto.Raft;

public class Application {

    public static void main(String[] args) {

        Raft raft = new Raft();
        raft.start();
    }
}
