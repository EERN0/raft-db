package ck.top.raft.server.proto;

import lombok.Data;

import java.io.Serializable;

@Data
public class ClientResponse implements Serializable {

    // 响应码 0-success，-1-failure
    int code;

    // 响应数据
    Object result;

    private ClientResponse(int code, Object result) {
        this.code = code;
        this.result = result;
    }

    public static ClientResponse success() {
        return new ClientResponse(0, null);
    }

    public static ClientResponse success(String value) {
        return new ClientResponse(0, value);
    }

    public static ClientResponse failure() {
        return new ClientResponse(-1, null);
    }
}
