package ck.top.raft.server.proto;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class Command implements Serializable {
    String key;
    String value;
}
