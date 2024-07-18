package ck.top.raft.client;

import ck.top.raft.server.proto.ClientRequest;
import ck.top.raft.server.proto.ClientResponse;
import ck.top.raft.server.rpc.Request;
import ck.top.raft.server.rpc.RpcClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KVClient {

    private final RpcClient client;

    private static List<String> peers;
    private int idx;
    private String addr;

    private static final int TIMEOUT_MS = 4000;
    private static final int RETRY_DELAY_MS = 300;

    static {
        peers = new ArrayList<>();
        Collections.addAll(peers, "localhost:9991", "localhost:9992", "localhost:9993", "localhost:9994", "localhost:9995");
    }

    public KVClient() {
        this.client = new RpcClient();
        this.idx = 0;
        this.addr = peers.get(idx);
    }

    public String get(String key) throws InterruptedException {
        // 客户端请求
        ClientRequest req = ClientRequest.builder().cmd(ClientRequest.GET).key(key).build();
        // 客户端响应
        ClientResponse response = null;

        // rpc请求
        Request rpcRequest = Request.builder().cmd(Request.CLIENT_REQUEST).obj(req).url(addr).build();

        // 发送请求直到成功
        while (response == null || response.getCode() != 1) {
            try {
                response = client.send(rpcRequest, TIMEOUT_MS);
            } catch (Exception e) {
                // 请求超时，访问另一个节点
                retryToNextPeer(rpcRequest);
                continue;
            }

            // 请求发送成功，但是处理失败 或者 需要重定向到leader
            if (response.getCode() == 0) {
                // server处理请求失败，重新发另一个节点
                retryToNextPeer(rpcRequest);
            } else if (response.getCode() == 2) {
                // 重定向请求到leader, 客户端响应被节点塞了leader地址
                addr = response.getData().toString();
                rpcRequest.setUrl(addr);
            }
        }

        return response.getData() == null ? null : response.getData().toString();
    }

    public String insert(String key, String value) throws InterruptedException {
        ClientRequest req = ClientRequest.builder().cmd(ClientRequest.INSERT).key(key).value(value).build();
        ClientResponse response = null;

        Request rpcRequest = Request.builder().cmd(Request.CLIENT_REQUEST).obj(req).url(addr).build();

        while (response == null || response.getCode() != 1) {
            try {
                response = client.send(rpcRequest, TIMEOUT_MS);
            } catch (Exception e) {
                // 请求超时，访问另一个节点
                retryToNextPeer(rpcRequest);
                continue;
            }

            // 请求发送成功，但是处理失败 或者 需要重定向到leader
            if (response.getCode() == 0) {
                // server处理请求失败，重新发另一个节点
                retryToNextPeer(rpcRequest);
            } else if (response.getCode() == 2) {
                // 重定向请求到leader, 客户端响应被节点塞了leader地址
                addr = response.getData().toString();
                rpcRequest.setUrl(addr);
            }
        }

        return "insert success";
    }

    public String delete(String key) throws InterruptedException {
        ClientRequest req = ClientRequest.builder().cmd(ClientRequest.DELETE).key(key).build();
        ClientResponse response = null;

        Request rpcRequest = Request.builder().cmd(Request.CLIENT_REQUEST).obj(req).url(addr).build();

        while (response == null || response.getCode() != 1) {
            try {
                response = client.send(rpcRequest, TIMEOUT_MS);
            } catch (Exception e) {
                // 请求超时，访问另一个节点
                retryToNextPeer(rpcRequest);
                continue;
            }

            // 请求发送成功，但是处理失败 或者 需要重定向到leader
            if (response.getCode() == 0) {
                // server处理请求失败，重新发另一个节点
                retryToNextPeer(rpcRequest);
            } else if (response.getCode() == 2) {
                // 重定向请求到leader, 客户端响应被节点塞了leader地址
                addr = response.getData().toString();
                rpcRequest.setUrl(addr);
            }
        }

        return "delete success";
    }

    /**
     * 请求发送给下一个peer
     *
     * @param request rpc请求
     */
    private void retryToNextPeer(Request request) throws InterruptedException {
        idx = (idx + 1) % peers.size();
        request.setUrl(peers.get(idx));
        Thread.sleep(RETRY_DELAY_MS);
    }
}
