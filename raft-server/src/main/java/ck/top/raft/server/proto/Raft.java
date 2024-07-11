package ck.top.raft.server.proto;

import ck.top.raft.common.constant.Constant;
import ck.top.raft.common.enums.Role;
import ck.top.raft.server.rpc.Request;
import ck.top.raft.server.rpc.RpcClient;
import ck.top.raft.server.rpc.RpcServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class Raft {
    // 当前节点地址，格式{ip:port}
    public String me;
    // 所有节点的地址
    public List<String> peers;
    // 当前节点状态
    public boolean isKilled = false;

    public ReentrantLock lock = new ReentrantLock();

    // 角色，默认为follower
    public volatile Role role = Role.FOLLOWER;
    // 节点当前的任期
    public volatile int currentTerm = 1;
    // 节点在当前任期投票给voteFor
    public volatile String votedFor;

    // 每个节点本地的日志。日志索引+日志任期相同 ==> 日志相同（从日志开头到索引位置的日志都相同）
    private List<LogEntry> logs;

    // 仅在leader中使用，每个peer节点的日志视图
    private Map<String, Integer> nextLogIndex;     // leader下一次给peer发送的日志，索引的起始为nextLogIndex.get(peer)
    private Map<String, Integer> matchLogIndex;    // matchLogIndex.get(peer): 节点peer已经复制的最高日志项的索引

    // 全局已提交日志的索引
    int commitLogIndex;
    // 已经应用的日志索引
    int lastAppliedIndex;

    // 选举开始时间
    public long electionStart;
    // 选举随机超时时间 2500~4000ms
    public long electionTimeout;

    // rpc
    private RpcServer rpcServer;
    private RpcClient rpcClient;

    // 线程池，执行领导选举逻辑
    private ThreadPoolExecutor electionExecutor;
    private ThreadPoolExecutor heartExecutor;
    // 定时任务
    private ScheduledExecutorService scheduler;

    private final AtomicBoolean isReplicationScheduled = new AtomicBoolean(false);


    // 心跳、日志复制定时任务
    private ReplicationTicker replicationTicker;

    public Raft() {
        electionStart = System.currentTimeMillis();
        replicationTicker = new ReplicationTicker();

        // 先加一条无效的空日志，类似虚拟头节点，减少边界判断
        logs = new ArrayList<>();
        logs.add(new LogEntry(Constant.INVALID_LOG_INDEX, Constant.INVALID_LOG_TERM));
    }

    public void start() {
        setConfig();
        threadPoolInit();
        // 重置选举超时时间
        resetElectionTimerLocked();

        log.info("Raft node started successfully. The current term is {}", currentTerm);

        CompletableFuture.runAsync(this::electionTicker);
    }

    public void setConfig() {
        String port = System.getProperty("server.port");
        me = "localhost:" + port;
        rpcServer = new RpcServer(Integer.parseInt(port), this);
        rpcClient = new RpcClient();
        peers = new ArrayList<>();
        peers.add("localhost:9991");
        peers.add("localhost:9992");
        peers.add("localhost:9993");
        peers.add("localhost:9994");
        peers.add("localhost:9995");

        // 初始化leader日志视图，leader掌握所有peer的日志视图
        nextLogIndex = new HashMap<>(peers.size());
        matchLogIndex = new HashMap<>(peers.size());

        // 初始化日志应用字段
        commitLogIndex = 0;
        lastAppliedIndex = 0;
    }

    private void threadPoolInit() {
        // 线程池参数
        int cpu = Runtime.getRuntime().availableProcessors();
        int maxPoolSize = cpu * 2;
        int queueSize = 1024;
        long keepAlive = 2000 * 60;

        //ThreadFactory namedThreadFactory = new ThreadFactory() {
        //    private final AtomicInteger threadNum = new AtomicInteger(1);
        //
        //    @Override
        //    public Thread newThread(Runnable r) {
        //        Thread t = new Thread(r);
        //        t.setName("election-thread-" + threadNum.getAndIncrement());
        //        return t;
        //    }
        //};
        ThreadFactory namedThreadFactory2 = new ThreadFactory() {
            private final AtomicInteger threadNum = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("schedule-task-thread-" + threadNum.getAndIncrement());
                return t;
            }
        };
        electionExecutor = new ThreadPoolExecutor(cpu, maxPoolSize, keepAlive, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(queueSize));
        //electionExecutor = new ThreadPoolExecutor(4, 4, keepAlive, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(queueSize));
        //electionExecutor = new NamedThreadPoolExecutor(cpu, maxPoolSize, keepAlive, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(queueSize), namedThreadFactory, "electionThreadPool");
        //heartExecutor = new NamedThreadPoolExecutor(cpu, maxPoolSize, keepAlive, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(queueSize), namedThreadFactory2, "electionThreadPool");
        scheduler = new ScheduledThreadPoolExecutor(1, namedThreadFactory2);
    }

    /**
     * 重置选举超时时间
     */
    private void resetElectionTimerLocked() {
        electionStart = System.currentTimeMillis();
        log.info("重置上一次选举时间: {}", electionStart);
        long range = Constant.ELECTION_TIMEOUT_MAX - Constant.ELECTION_TIMEOUT_MIN;
        // 选举超时时间：2500~4000ms
        electionTimeout = Constant.ELECTION_TIMEOUT_MIN + ThreadLocalRandom.current().nextLong() % range;
    }

    /**
     * 判断选举是否超时
     *
     * @return true-超时，false-没超时
     */
    private boolean isElectionTimeoutLocked() {
        long l = System.currentTimeMillis() - (electionStart + electionTimeout);
        if (l > 0) {
            log.info("选举超时，超时时间:{}", l);
        } else {
            log.info("选举未超时");
        }
        return System.currentTimeMillis() - electionStart > electionTimeout;
    }

    /**
     * 比较当前节点的日志、候选者candidate的日志的新旧
     * 谁的日志term大，谁的日志更新；日志term相同，日志索引大的日志更新
     *
     * @param candidateLogIndex 候选者日志idx
     * @param candidateLogTerm  候选者日志任期
     * @return true--candidate的日志比当前节点的日志新
     */
    private boolean isMoreUpToDateLocked(int candidateLogIndex, int candidateLogTerm) {
        int len = logs.size();
        // 当前节点最后一条日志
        int lastLogIndex = len - 1, lastLogTerm = logs.get(len - 1).getTerm();
        // 当前节点最新日志的任期 (lastTerm) 与候选者的任期 (candidateTerm) 不同，日志任期大的新
        if (lastLogTerm != candidateLogTerm) {
            return lastLogTerm > candidateLogTerm;
        }
        // 两日志任期相同，通过索引比较日志的新旧
        return lastLogIndex > candidateLogIndex;
    }

    /**
     * 其它节点rpc请求中任期（term）更大，当前节点角色变为follower
     *
     * @param term 其它节点任期
     */
    public void becomeFollowerLocked(int term) {
        if (term < currentTerm) {
            log.info("Can't become Follower, 丢弃低任期请求, lower term: T{}", term);
        }
        log.info("{}->Follower, for T{}->T{}", role.getDesc(), currentTerm, term);
        role = Role.FOLLOWER;

        // TODO 节点 currentTerm || votedFor || log改变，都需要持久化

        // 其它节点任期 大于 当前节点
        if (term > currentTerm) {
            votedFor = null;
        }
        currentTerm = term;
    }

    /**
     * 变为candidate，投票给自己
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
        for (String peer : peers) {
            nextLogIndex.put(peer, logs.size());
            matchLogIndex.put(peer, 0);
        }
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
     * 回调函数
     * 接收方收到candidate的要票rpc请求，执行该回调
     *
     * @param args 要票rpc请求参数
     * @return RequestVoteReply 要票rpc响应
     */
    public RequestVoteReply requestVote(RequestVoteArgs args) {
        try {
            lock.lock();
            log.debug("被要票方: {}, Args={}", args.getCandidateId(), args.toPrettyString());

            RequestVoteReply reply = new RequestVoteReply();
            reply.setTerm(currentTerm);
            reply.setVoteGranted(false);

            // 要票节点的任期 小于 当前节点（被要票的一方）任期，无效的要票请求，当前节点拒绝投票
            if (args.getTerm() < currentTerm) {
                log.info("要票节点 {} 任期 T{} 小，当前节点 {} 任期 T{} 大，拒绝这个请求", args.getCandidateId(), args.getTerm(), me, currentTerm);
                return reply;
            }
            // 要票节点的任期 > 当前节点任期，当前节点变为follower
            if (args.getTerm() > currentTerm) {
                log.info("变为follower++++++++++++++");
                log.info("candidate-{}-T{} > 被要票节点-{}-T{}", args.getCandidateId(), args.getTerm(), me, currentTerm);
                becomeFollowerLocked(args.getTerm());
            }
            // 当前节点投过票
            if (StringUtils.isNotEmpty(votedFor)) {
                log.info("candidate-{}-T{} 拒绝接受投票，被要票节点-{} 已经投票给 {}", args.getCandidateId(), args.getTerm(), me, votedFor);
                return reply;
            }

            // 接收方节点的日志 新于 要票方candidate的日志
            if (isMoreUpToDateLocked(args.getLastLogIndex(), args.getLastLogTerm())) {
                return reply;
            }

            // 接收方(当前节点) 投给 要票节点一票
            reply.setVoteGranted(true);
            votedFor = args.getCandidateId();

            // TODO 节点 currentTerm || votedFor || log改变，都需要持久化 persistLocked()
            // 重置选举超时时间
            resetElectionTimerLocked();

            log.info("要票节点 {} 获得投票", args.getCandidateId());
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
    public AppendEntriesReply appendEntries(AppendEntriesArgs args) {
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
                log.info("{}-{}-T{} 收到心跳rpc请求，认可{}-Leader-T{}的地位", me, role, currentTerm, args.getLeaderId(), args.getTerm());
                becomeFollowerLocked(args.getTerm());
            }
            // 是leader发来的心跳rpc，更新本地任期
            reply.setTerm(args.getTerm());
            reply.setSucceeded(true);
            // TODO 日志复制

            // 接受心跳rpc的节点(当前节点)认可对方是leader，重置自己的选举时钟，一段时间内不发起选举
            // 不管日志匹配是否成功，避免leader和follower匹配日志时间过长
            resetElectionTimerLocked();
            return reply;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 领导者选举的定时任务
     * 当前节点不是leader且选举超时，节点成为candidate，发起选举
     */
    private void electionTicker() {
        while (true) {
            lock.lock();
            try {
                // 不是leader 且 选举超时了，节点成为candidate，发起选举
                if (role != Role.LEADER && isElectionTimeoutLocked()) {
                    becomeCandidateLocked();
                    log.info("发起选举");
                    CompletableFuture.runAsync(() -> startElection(currentTerm), electionExecutor);
                }
            } finally {
                lock.unlock();
            }
            try {
                int rand = new Random().nextInt(300) + 300;
                Thread.sleep(rand);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void startElection(long term) {
        // 投票数
        AtomicInteger votes = new AtomicInteger(0);
        for (String peer : peers) {
            // 是自己，给自己投一票
            if (Objects.equals(peer, me)) {
                votes.incrementAndGet();
                continue;
            }
            RequestVoteArgs args = new RequestVoteArgs();
            args.setTerm(currentTerm);
            args.setCandidateId(me);

            // 上下文检查（检查当前节点还是不是发送rpc要票请求之前的角色，因为rpc请求响应的时间长，避免要票节点角色在rpc期间发生变化）
            // 在发送rpc请求和响应的这个时间段内，检查节点角色和任期是否变化（candidate才会发要票rpc请求）
            if (contextLostLocked(Role.CANDIDATE, term)) {
                log.info("当前节点角色: {}, Lost context, abort RequestVoteReply for {}", role, peer);
                return;
            }

            CompletableFuture.supplyAsync(() -> askVoteFromPeer(peer, args), electionExecutor)
                    .thenAcceptAsync(reply -> {
                        lock.lock();
                        // 响应的任期 大于 要票节点任期，要票节点变为follower；否则 响应节点投给要票节点一票
                        try {
                            if (reply == null) {
                                log.info(String.format("Ask vote from %s, Lost or error", peer));
                                return;
                            }
                            if (reply.getTerm() > currentTerm) {
                                log.info("变为follower---------------------");
                                becomeFollowerLocked(reply.getTerm());
                                return;
                            }
                            // 统计选票
                            if (reply.isVoteGranted()) {
                                // 超过半数选票，成为leader
                                if (votes.incrementAndGet() > peers.size() / 2) {
                                    // TODO 第一次变为leader执行，只执行一次
                                    // 成为leader，异步执行定时任务，发送【心跳和日志同步】rpc
                                    log.info("{}-T{}成为leader, 获得票数{}", args.getCandidateId(), currentTerm, votes.get());
                                    becomeLeaderLocked();
                                    if (isReplicationScheduled.compareAndSet(false, true)) {
                                        scheduler.scheduleWithFixedDelay(replicationTicker, 0, Constant.REPLICATE_INTERVAL, TimeUnit.MILLISECONDS);
                                    }
                                }
                            }
                        } finally {
                            lock.unlock();
                        }
                    }, electionExecutor);
        }
    }

    // 要票rpc请求，执行对应回调
    private RequestVoteReply askVoteFromPeer(String peer, RequestVoteArgs args) {
        RequestVoteReply reply;
        try {
            Request request = new Request(Request.REQUEST_VOTE, args, peer);
            reply = rpcClient.send(request);
            return reply;
        } catch (Exception e) {
            log.error("Failed to send vote request to peer: {}", peer, e);
            return null;
        }
    }


    /**
     * 心跳、日志复制的定时任务
     * 当前节点是leader，向其它follower定时发送心跳、日志复制请求
     */
    class ReplicationTicker implements Runnable {
        @Override
        public void run() {
            if (role == Role.LEADER) {
                startReplication(currentTerm);
            }
        }

        private void startReplication(int term) {
            List<CompletableFuture<AppendEntriesReply>> futures = new ArrayList<>();

            for (String peer : peers) {
                if (Objects.equals(peer, me)) {
                    continue;
                }
                AppendEntriesArgs args = new AppendEntriesArgs();
                args.setLeaderId(me);
                args.setTerm(term);

                // 上下文检查，异步请求和响应之间，检查当前节点角色、任期是否改变
                if (contextLostLocked(Role.LEADER, term)) {
                    log.info("当前节点角色: {}, Lost context, abort RequestVoteReply for {}", role, peer);
                    return;
                }

                // 对每个peer发送心跳、日志复制rpc
                CompletableFuture.supplyAsync(() -> replicateToPeer(peer, args), electionExecutor)
                        .thenAcceptAsync(reply -> {
                            lock.lock();
                            try {
                                if (reply == null) {
                                    return;
                                }
                                if (reply.getTerm() > currentTerm) {
                                    log.info("心跳响应的任期 T{} 大于 {}-{}的任期 T{}", reply.getTerm(), me, role, currentTerm);
                                    becomeFollowerLocked(reply.getTerm());
                                    return;
                                }
                                log.info("{}-{}-T{} 收到来自 {}-T{} 的心跳响应", args.getLeaderId(), role, args.getTerm(), peer, reply.getTerm());

                                // TODO 处理leader和follower日志不匹配的情况（索引或任期不同）
                            } finally {
                                lock.unlock();
                            }
                        }, electionExecutor);

                //CompletableFuture<AppendEntriesReply> future = CompletableFuture.supplyAsync(() -> replicateToPeer(peer, args), electionExecutor);
                //futures.add(future);
            }
            //for (CompletableFuture<AppendEntriesReply> future : futures) {
            //
            //    AppendEntriesReply reply = future.join();
            //    appendLock.lock();
            //    try {
            //        if (reply == null) {
            //            return;
            //        }
            //        if (reply.getTerm() > currentTerm) {
            //            log.info("follower任期T{} 大于 leader任期T{}, {}->follower", reply.getTerm(), currentTerm, role);
            //            becomeFollowerLocked(reply.getTerm());
            //            return;
            //        }
            //        log.info("{}-{} 收到来自 {} 的心跳响应", me, role, "xxx");
            //
            //        // TODO 处理leader和follower日志不匹配的情况（索引或任期不同）
            //    } finally {
            //        appendLock.unlock();
            //    }
            //}
        }
    }

    // 心跳、日志复制rpc请求，执行对应回调
    private AppendEntriesReply replicateToPeer(String peer, AppendEntriesArgs args) {
        AppendEntriesReply reply;
        try {
            Request request = new Request(Request.APPEND_ENTRIES, args, peer);
            reply = rpcClient.send(request);
            log.info("{}-{}-T{} 发送心跳rpc 给 {}-T{}", args.getLeaderId(), role, args.getTerm(), peer, reply.getTerm());

            //log.info("leader-{}-T{} 给 {}-T{} 发送心跳rpc", args.getLeaderId(), args.getTerm(), peer, reply.getTerm());
            return reply;
        } catch (Exception e) {
            return null;
        }
    }
}