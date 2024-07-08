package ck.top.raft.server.rpc;

import com.alipay.remoting.exception.RemotingException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RpcClient {

    private final com.alipay.remoting.rpc.RpcClient rpcClient;

    public RpcClient() {
        rpcClient = new com.alipay.remoting.rpc.RpcClient();
        rpcClient.startup();
    }

    public <R> R send(Request request) throws RemotingException, InterruptedException {
        return send(request, 100);
    }

    public <R> R send(Request request, int timeout) throws RemotingException, InterruptedException {
        Response<R> result;
        result = (Response<R>) rpcClient.invokeSync(request.getUrl(), request, timeout);
        return result.getResult();
    }

    public void destroy() {
        rpcClient.shutdown();
    }
}
