package ck.top.raft.server.rpc;

import lombok.Data;

import java.io.Serializable;

@Data
public class Response<T> implements Serializable {

    private T result;

    public Response(T result) {
        this.result = result;
    }

}