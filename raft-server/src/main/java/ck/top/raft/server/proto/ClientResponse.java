package ck.top.raft.server.proto;

import lombok.Data;

import java.io.Serializable;

@Data
public class ClientResponse implements Serializable {

    // 响应码 0-failure, 1-success，2-重定向至leader
    int code;
    // 响应数据
    Object data;
    // 描述信息
    String desc;

    private ClientResponse(int code, Object data, String desc) {
        this.code = code;
        this.data = data;
        this.desc = desc;
    }

    public static ClientResponse failure(String desc) {
        return new ClientResponse(0, null, desc);
    }

    public static ClientResponse success() {
        return new ClientResponse(1, null, "请求处理成功");
    }

    public static ClientResponse success(String value) {
        return new ClientResponse(1, value, "请求处理成功，返回值: " + value);
    }

    // 客户端请求重定向，发送到leader
    public static ClientResponse redirect(String leader) {
        return new ClientResponse(2, leader, "请求重定向至leader: " + leader);
    }
}
