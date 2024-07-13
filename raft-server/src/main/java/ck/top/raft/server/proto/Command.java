package ck.top.raft.server.proto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class Command implements Serializable {
    String key;
    String value;
}
