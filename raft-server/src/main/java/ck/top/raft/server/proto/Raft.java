package ck.top.raft.server.proto;

import ck.top.raft.common.constant.Constant;
import ck.top.raft.common.enums.Role;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class Raft {
    // 当前节点地址，格式{ip:port}
    public String me;
    // 所有节点的地址
    public List<String> peers;
    // 当前节点状态
    public boolean killed = false;

    public ReentrantLock lock = new ReentrantLock();

    // 角色，默认为follower
    public volatile Role role = Role.FOLLOWER;
    // 节点当前的任期
    public volatile long currentTerm;
    // 节点在当前任期投票给voteFor
    public volatile String votedFor;

    // 选举开始时间
    public long electionStart;
    // 选举随机超时时间
    public long electionTimeout;

    public Raft() {
        electionStart = System.currentTimeMillis();
    }

    /**
     * 其它节点rpc请求中任期（term）更大，当前节点角色变为follower
     *
     * @param term 其它节点任期
     */
    public void becomeFollowerLocked(long term) {
        if (term < currentTerm) {
            log.info("Can't become Follower, lower term: T{}", term);
        }
        log.info("{}->Follower, for T{}->T{}", role.getDesc(), currentTerm, term);
        role = Role.FOLLOWER;

        // TODO 节点 currentTerm || votedFor || log改变，都需要持久化

        // 其它任期 大于 当前节点
        if (term > currentTerm) {
            votedFor = null;
        }
        currentTerm = term;
    }

    /**
     * 变为Candidate，投票给自己
     */
    public void becomeCandidateLocked() {
        if (role.getCode() == Role.LEADER.getCode()) {
            log.info("Leader can't become Candidate");
            return;
        }
        log.info("{}->Candidate, for T{}", role.getDesc(), currentTerm + 1);
        currentTerm++;
        role = Role.CANDIDATE;
        votedFor = me;
        // TODO 节点 currentTerm || votedFor || log改变，都需要持久化
    }

    /**
     * 变为leader
     */
    public void becomeLeaderLocked() {
        if (role != Role.CANDIDATE) {
            log.info("Only Candidate can become Leader");
            return;
        }
        log.info("Become Leader in T{}", currentTerm);
        role = Role.LEADER;

        // TODO rf当选leader，初始化leader节点中维护的peers日志视图
    }

    /**
     * 节点上下文是否改变（角色、任期）
     *
     * @return true-改变，false-没改变
     */
    public boolean contextLostLocked(Role role, long term) {
        return !(role == this.role && term == currentTerm);
    }

    /**
     * 重置选举超时时间
     */
    private void resetElectionTimerLocked() {
        lock.lock();
        try {
            long range = Constant.ELECTION_TIMEOUT_MAX - Constant.ELECTION_TIMEOUT_MIN;
            electionTimeout = Constant.ELECTION_TIMEOUT_MIN + new Random().nextLong() % range;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 判断选举是否超时
     *
     * @return true-超时，false-没超时
     */
    private boolean isElectionTimeoutLocked() {
        lock.lock();
        try {
            return System.currentTimeMillis() - electionStart > electionTimeout;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 要票rpc请求的回调函数
     * 接收方执行
     *
     * @param args 要票rpc请求参数
     * @return RequestVoteReply 要票rpc响应
     */
    public RequestVoteReply requestVote(RequestVoteArgs args) {
        try {
            lock.lock();
            log.debug("<- S{}, rpc-VoteAsked, Args={}", args.getCandidateId(), args.toPrettyString());

            RequestVoteReply reply = new RequestVoteReply();
            reply.setTerm(currentTerm);
            reply.setVoteGranted(false);

            // 要票节点的任期 小于 当前节点任期，无效的要票请求，当前拒绝投票
            if (args.getTerm() < currentTerm) {
                log.info("<- S{}, Reject voted, higher term, T{}>T{}", args.getCandidateId(), currentTerm, args.getTerm());
                return reply;
            }
            // 要票节点的任期 > 当前节点任期，当前节点变为follower
            if (args.getTerm() > currentTerm) {
                role = Role.FOLLOWER;
                currentTerm = args.getTerm();
            }
            // 当前节点投过票
            if (StringUtils.isNotEmpty(votedFor)) {
                log.info("<- S{}, Reject, Already voted for S{}", args.getCandidateId(), votedFor);
                return reply;
            }

            // 接收方(当前节点) 投给 要票节点一票
            reply.setVoteGranted(true);
            votedFor = args.getCandidateId();

            // TODO 节点 currentTerm || votedFor || log改变，都需要持久化 persistLocked()
            // 重置选举超时时间
            resetElectionTimerLocked();

            log.info("<- S{}, Vote granted", args.getCandidateId());
            return reply;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 回调函数
     * 接收方(follower)收到leader发来的心跳、日志复制rpc请求后，执行该回调
     *
     * @param args 心跳、日志复制rpc请求参数
     * @return AppendEntriesReply 心跳、日志复制响应
     */
    public AppendEntriesReply AppendEntries(AppendEntriesArgs args) {
        try {
            lock.lock();
            AppendEntriesReply reply = new AppendEntriesReply();
            reply.setTerm(currentTerm);
            reply.setSucceeded(false);

            // 检查任期，心跳rpc的任期 和 当前节点(接收方)的任期
            if (args.getTerm() < currentTerm) {
                // rpc请求中任期过小，丢弃这个请求
                return reply;
            } else {
                // args.getTerm() >= currentTerm, 接收方变为follower，维护leader的地位
                becomeFollowerLocked(args.getTerm());
            }
            // 是leader发来的心跳rpc，更新本地任期
            reply.setTerm(args.getTerm());
            reply.setSucceeded(true);
            // TODO 日志复制

            // 只要认可对方是leader就要重置选举时钟，不管日志匹配是否成功，避免leader和follower匹配日志时间过长
            resetElectionTimerLocked();
            return reply;
        } finally {
            lock.unlock();
        }
    }


}
