package ck.top.raft.server.rpc;

import ck.top.raft.server.proto.AppendEntriesArgs;
import ck.top.raft.server.proto.ClientRequest;
import ck.top.raft.server.proto.Raft;
import ck.top.raft.server.proto.RequestVoteArgs;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.SyncUserProcessor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RpcServer {

    private final Raft rf;

    private final com.alipay.remoting.rpc.RpcServer server;

    public RpcServer(int port, Raft rf) {
        // 初始化rpc服务端
        server = new com.alipay.remoting.rpc.RpcServer(port, false, false);

        // 实现用户请求处理器
        server.registerUserProcessor(new SyncUserProcessor<Request>() {
            @Override
            public Object handleRequest(BizContext bizContext, Request request) throws Exception {
                return handlerRequest(request);
            }

            @Override
            public String interest() {
                return Request.class.getName();
            }
        });
        this.rf = rf;
        server.startup();
    }

    /**
     * 请求参数中服务方法名 对应 相应的回调函数
     *
     * @param request 请求参数
     */
    public Response<?> handlerRequest(Request request) {
        if (request.getCmd() == Request.REQUEST_VOTE) {
            // 处理来自其它节点的投票请求，决定是否投票
            return new Response<>(rf.requestVote((RequestVoteArgs) request.getObj()));
        } else if (request.getCmd() == Request.APPEND_ENTRIES) {
            return new Response<>(rf.appendEntries((AppendEntriesArgs) request.getObj()));
        } else if (request.getCmd() == Request.CLIENT_REQUEST) {
            return new Response<>(rf.clientRequest((ClientRequest) request.getObj()));
        }
        return null;
    }

    public void destroy() {
        server.shutdown();
    }
}
