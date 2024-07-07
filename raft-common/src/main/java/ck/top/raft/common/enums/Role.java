package ck.top.raft.common.enums;

import lombok.Getter;

@Getter
public enum Role {
    UNKNOWN(0, "Unknown"),
    FOLLOWER(1, "Follower"),
    CANDIDATE(2, "Candidate"),
    LEADER(3, "Leader");

    private final int code;
    private final String desc;

    Role(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }
}
