package ck.top.raft.server.proto;

import ck.top.raft.common.constant.Constant;
import ck.top.raft.common.enums.Role;
import ck.top.raft.server.kv.SkipList;
import ck.top.raft.server.log.LogEntry;
import ck.top.raft.server.rpc.Request;
import ck.top.raft.server.rpc.RpcClient;
import ck.top.raft.server.rpc.RpcServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class Raft {
    // 当前节点地址，格式{ip:port}
    private String me;
    // 所有节点的地址
    private List<String> peers;
    // 当前任期的leader的地址
    private String leaderId;

    private final ReentrantLock lock = new ReentrantLock();

    // 角色，默认为follower
    private volatile Role role = Role.FOLLOWER;
    // 节点当前的任期
    private volatile int currentTerm = 1;
    // 节点在当前任期投票给voteFor
    private volatile String votedFor;

    // 状态持久化
    private RaftState raftState;
    private Persister persister;

    // 每个节点本地的日志。日志索引+日志任期相同 ==> 日志相同（从日志开头到索引位置的日志都相同）
    private List<LogEntry> logs;

    // 仅在leader中使用，每个peer的日志视图
    private Map<String, Integer> nextLogIndex;     // leader下一次给peer发送的日志，起始索引为nextLogIndex.get(peer)
    private Map<String, Integer> matchLogIndex;    // matchLogIndex.get(peer): peer节点已经成功复制的日志条目最高索引

    // 全局已提交日志的索引，表示所有在此索引或更低索引的日志条目都已经被大多数节点成功复制，并且可以被安全地应用到状态机中
    int commitLogIndex;
    // 已经应用的日志索引
    int lastAppliedIndex;

    // 日志应用的条件变量
    private final Condition applyCond = lock.newCondition();
    // 传递要应用到状态机的日志
    private BlockingQueue<ApplyMsg> applyCh = new LinkedBlockingQueue<>();

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

    // KV存储引擎
    SkipList<String, String> skipList = SkipList.getInstance();

    // 心跳、日志复制定时任务
    private final ReplicationTicker replicationTicker;

    public Raft() {
        electionStart = System.currentTimeMillis();
        replicationTicker = new ReplicationTicker();

        // 先加一条无效的空日志，类似虚拟头节点，减少边界判断
        logs = new ArrayList<>();
        logs.add(new LogEntry(Constant.INVALID_LOG_INDEX, Constant.INVALID_LOG_TERM));

        raftState = RaftState.builder().votedFor(votedFor).currentTerm(currentTerm).logs(logs).build();
    }

    public void start() {
        setConfig();
        threadPoolInit();
        // 重置选举超时时间
        resetElectionTimerLocked();

        log.info("Raft node started successfully. The current term is {}", currentTerm);

        CompletableFuture.runAsync(this::electionTicker);

        CompletableFuture.runAsync(this::applicationTicker);
    }

    public void setConfig() {
        String port = System.getProperty("server.port");
        me = "localhost:" + port;
        rpcServer = new RpcServer(Integer.parseInt(port), this);
        rpcClient = new RpcClient();
        peers = new ArrayList<>();
        Collections.addAll(peers, "localhost:9991", "localhost:9992", "localhost:9993", "localhost:9994", "localhost:9995");

        // raft节点状态恢复
        this.persister = new Persister(port);
        byte[] data = persister.readRaftState();
        if (data != null) {
            try {
                this.raftState = (RaftState) Persister.deserialize(data);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        } else {
            this.raftState = new RaftState(1, null, new ArrayList<>());
        }

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

        ThreadFactory namedThreadFactory2 = new ThreadFactory() {
            private final AtomicInteger threadNum = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("schedule-task-thread-" + threadNum.getAndIncrement());
                return t;
            }
        };
        electionExecutor = new ThreadPoolExecutor(cpu, maxPoolSize, keepAlive, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(queueSize));
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

        // 节点 currentTerm || votedFor || log改变，都需要持久化
        persistLocked();

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
        // 节点 currentTerm || votedFor || log改变，都需要持久化
        persistLocked();
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
        leaderId = me;
        // rf当选leader，初始化leader节点中维护的peers日志视图
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
    public boolean contextLostLocked(Role role, int term) {
        return !(role == this.role && term == currentTerm);
    }

    /**
     * 节点第一条任期为term的日志索引
     *
     * @param term 任期
     * @return 第一条任期为term的日志索引 or 无效日志索引
     */
    public int firstLogIndexFor(int term) {
        for (int i = 0; i < logs.size(); i++) {
            if (logs.get(i).getTerm() == term) {
                return i;
            } else if (logs.get(i).getTerm() > term) {
                break;
            }
        }
        return Constant.INVALID_LOG_INDEX;
    }

    /**
     * leader维护了每个peer已同步的日志索引matchLogIndex
     * 使用排序后找中位数的方式，计算多数peer的匹配点，中位数表示有一半以上的节点已经成功复制了该索引或更低索引的日志条目
     */
    public int getMajorityMatchedLogIndexLocked() {
        // 所有节点已经同步的日志索引
        List<Integer> temp = new ArrayList<>(matchLogIndex.values());
        // 排序后的中位数(中左)
        Collections.sort(temp);
        int majorityIdx = peers.size() / 2;
        log.info("Matched index after sort: {}, majority[{}]={}", temp, majorityIdx, temp.get(majorityIdx));
        return temp.get(majorityIdx);
    }

    /**
     * 状态持久化
     */
    public void persistLocked() {
        raftState.setCurrentTerm(currentTerm);
        raftState.setVotedFor(votedFor);
        raftState.setLogs(logs);
        try {
            byte[] data = Persister.serialize(raftState);
            persister.save(data, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
                log.info("当前节点-{}-T{}的最后一条日志索引{} > {}-T{}的日志索引{}，拒绝投票给{}",
                        me, currentTerm, logs.size() - 1, args.getCandidateId(), args.getTerm(), args.getLastLogIndex(), args.getCandidateId());
                return reply;
            }

            // 接收方(当前节点) 投给 要票节点一票
            reply.setVoteGranted(true);
            votedFor = args.getCandidateId();

            // 节点 currentTerm || votedFor || log改变，都需要持久化 persistLocked()
            persistLocked();

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

            // 检查任期，心跳rpc的任期 和 本地节点(接收方follower)的任期
            if (args.getTerm() < currentTerm) {
                // rpc请求中任期过小，丢弃这个请求
                return reply;
            } else {
                // args.getTerm() >= currentTerm, 接收方变为follower，维护leader的地位
                log.info("{}-{}-T{} 收到心跳rpc请求，认可{}-Leader-T{}的地位", me, role, currentTerm, args.getLeaderId(), args.getTerm());
                becomeFollowerLocked(args.getTerm());
            }

            // 本地接受心跳rpc，认可对方是leader，重置自己的选举时钟，一段时间内不发起选举
            // 不管日志匹配是否成功，避免leader和follower匹配日志时间过长
            resetElectionTimerLocked();
            leaderId = args.getLeaderId();

            // 日志冲突(日志不匹配)：日志的索引 或 任期 不同
            // 1、leader前一条日志索引 超出 本地节点的日志范围，说明leader倒数第二条日志就比follower的多
            if (args.getPreLogIndex() >= logs.size()) {
                // 设置冲突日志为无效任期0，冲突日志索引为logs.size()，下次让leader发送冲突索引及之后的所有日志条目，用来同步follower日志
                reply.setConflictLogTerm(Constant.INVALID_LOG_TERM);
                reply.setConflictLogIndex(logs.size());
                return reply;
            }
            // 2、leader前一条日志的任期 不等于 本地节点的日志任期
            if (args.getPreLogTerm() != logs.get(args.getPreLogIndex()).getTerm()) {
                reply.setConflictLogTerm(logs.get(args.getPreLogIndex()).getTerm());
                reply.setConflictLogIndex(firstLogIndexFor(reply.getConflictLogTerm()));
                return reply;
            }
            // leader前一条日志匹配(索引idx)上了本地日志(索引idx)，本地节点同步leader的日志
            // 清空本地日志[idx+1, size-1]的日志，复制leader传入的日志条目
            logs.subList(args.getPreLogIndex() + 1, logs.size()).clear();
            logs.addAll(args.getPreLogIndex() + 1, args.getEntries());
            // 状态持久化
            persistLocked();

            log.info("本地节点{}-{}-T{}日志内容: {}", role, me, currentTerm, logs.toString());

            reply.setTerm(args.getTerm());
            reply.setSucceeded(true);

            // 本地节点根据leader最新的commit来更新自己的commit信息
            if (args.getLeaderCommitIndex() > commitLogIndex) {
                commitLogIndex = args.getLeaderCommitIndex();
                // 确保commit日志索引不会超过
                if (commitLogIndex >= logs.size()) {
                    commitLogIndex = logs.size() - 1;
                }
            }
            // 唤醒 日志应用 开始干活
            applyCond.signal();

            return reply;
        } finally {
            lock.unlock();
        }
    }


    /**
     * 回调函数
     * leader处理客户端请求，把客户端命令转成日志，应用到状态机
     * 客户端的请求
     */
    public ClientResponse clientRequest(ClientRequest req) {
        // TODO: reader index机制，客户端能从follower读取数据且保证一致性，减少leader负担
        if (role != Role.LEADER) {
            if (StringUtils.isNotEmpty(leaderId)) {
                log.info("当前节点{}-{} 不是leader, 客户端请求重定向至leader-{}", role.getDesc(), me, leaderId);
                return ClientResponse.redirect(leaderId);
            } else {
                log.warn("当前任期 T{} raft集群没有leader", currentTerm);
                return ClientResponse.failure("当前任期raft集群没有leader");
            }
        }
        // leader处理客户端请求
        if (req.getCmd() == ClientRequest.GET) {
            log.info("我是{}，我来处理客户端get请求", role);
            lock.lock();
            try {
                String value = skipList.get(req.getKey());
                if (value != null) {
                    return ClientResponse.success(value);
                } else {
                    return ClientResponse.success();
                }
            } finally {
                lock.unlock();
            }
        }
        if (req.getCmd() == ClientRequest.INSERT) {
            log.info("我是{}，我来处理客户端insert请求", role);
            lock.lock();
            try {
                LogEntry newLog = LogEntry.builder().term(currentTerm).index(logs.size() - 1).isValid(true)
                        .command(Command.builder().key(req.getKey()).value(req.getValue()).build())
                        .build();
                logs.add(newLog);
                // TODO 日志应用
                skipList.insert(req.getKey(), req.getValue());
                return ClientResponse.success();
            } finally {
                lock.unlock();
            }
        }

        return null;
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

    private void startElection(int term) {
        // 投票数
        AtomicInteger votes = new AtomicInteger(0);
        // 当前节点的日志长度
        int len = logs.size();

        for (String peer : peers) {
            // 是自己，给自己投一票
            if (Objects.equals(peer, me)) {
                votes.incrementAndGet();
                continue;
            }
            // candidate的要票rpc请求参数，得加上日志部分
            RequestVoteArgs args = RequestVoteArgs.builder()
                    .term(currentTerm).candidateId(me).lastLogIndex(len - 1).lastLogTerm(logs.get(len - 1).getTerm())
                    .build();

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
                                    // 成为leader，异步执行定时任务，发送【心跳和日志同步】rpc (第一次变为leader执行，只执行一次)
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
            for (String peer : peers) {
                if (Objects.equals(peer, me)) {
                    matchLogIndex.put(peer, logs.size() - 1);
                    nextLogIndex.put(peer, logs.size());
                    continue;
                }

                int prevIdx = nextLogIndex.get(peer) - 1;
                int prevTerm = logs.get(prevIdx).getTerm();

                AppendEntriesArgs args = new AppendEntriesArgs();
                args.setLeaderId(me);
                args.setTerm(term);
                args.setPreLogIndex(prevIdx);
                args.setPreLogTerm(prevTerm);
                args.setEntries(new ArrayList<>(logs.subList(prevIdx + 1, logs.size())));
                args.setLeaderCommitIndex(commitLogIndex);

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

                                // 处理leader和follower日志冲突
                                if (!reply.isSucceeded()) {
                                    // leader下一次发给peer日志复制的起始索引
                                    Integer nextIdx = nextLogIndex.get(peer);

                                    // 1、follower返回的冲突任期是无效任期，follower日志数量少了(leader倒数第二条及以前的日志数 多于 follower的日志数)
                                    if (reply.getConflictLogTerm() == Constant.INVALID_LOG_TERM) {
                                        // leader下次发给peer的日志同步请求中，起始索引为follower最后一条日志索引+1，以便从follower日志的末尾开始同步日志
                                        nextIdx = reply.getConflictLogIndex();
                                    } else {
                                        // 2、日志数量没少，但是任期没对上，说明leader和follower的日志在某个点之后不一样了，跳过冲突任期的所有日志
                                        // raft中，一段连续的相同任期的日志条目要么全部匹配，要么全部不匹配。日志不匹配，回退当前任期的全部日志，直到前面任期不同的日志或者移动到日志的起始位置
                                        // leader查找冲突任期的第一条日志
                                        int firstLogIdx = firstLogIndexFor(reply.getConflictLogTerm());
                                        if (firstLogIdx != Constant.INVALID_LOG_INDEX) {
                                            // leader下一次的日志同步，把所有冲突任期的日志都发一遍
                                            nextLogIndex.put(peer, firstLogIdx);
                                        } else {
                                            // leader日志中不存在冲突任期的任何日志，以follower的日志为准跳过冲突任期
                                            nextLogIndex.put(peer, reply.getConflictLogIndex());
                                        }
                                    }
                                    // 存在网络分区，避免超时的响应到达，更新了nextIdx
                                    if (nextLogIndex.get(peer) > nextIdx) {
                                        nextLogIndex.put(peer, nextIdx);
                                    }
                                    return;
                                }
                                // leader跟踪每个peer日志复制的进度，follower完成日志同步后(成功追加了日志)，leader更新matchLogIndex和nextLogIndex
                                matchLogIndex.put(peer, args.getPreLogIndex() + args.getEntries().size());  // follower已经复制日志最后的索引
                                nextLogIndex.put(peer, matchLogIndex.get(peer) + 1);                        // leader下一次发送日志同步请求的索引

                                // leader确定日志条目已经被大多数节点成功复制时，标记该日志条目为已提交，更新commitIndex
                                int majorityMatchedIdx = getMajorityMatchedLogIndexLocked();
                                if (majorityMatchedIdx > commitLogIndex && logs.get(majorityMatchedIdx).getTerm() == currentTerm) {
                                    log.info("Leader update the commit index {}->{}", commitLogIndex, majorityMatchedIdx);
                                    // leader更新commitLogIndex为多数派的匹配点
                                    commitLogIndex = majorityMatchedIdx;
                                    // 一旦commitLogIndex被更新，leader就会将所有索引小于等于commitLogIndex的日志标记为已提交，应用到leader的状态机
                                    applyCond.signal();
                                }
                            } finally {
                                lock.unlock();
                            }
                        }, electionExecutor);
            }
        }

        // 心跳、日志复制rpc请求，执行对应回调
        private AppendEntriesReply replicateToPeer(String peer, AppendEntriesArgs args) {
            AppendEntriesReply reply;
            try {
                Request request = new Request(Request.APPEND_ENTRIES, args, peer);
                reply = rpcClient.send(request);
                log.info("{}-{}-T{} 发送心跳rpc 给 {}-T{}", args.getLeaderId(), role, args.getTerm(), peer, reply.getTerm());
                return reply;
            } catch (Exception e) {
                return null;
            }
        }
    }

    private void applicationTicker() {
        while (true) {
            lock.lock();
            try {
                List<LogEntry> entries = new ArrayList<>();
                for (int i = lastAppliedIndex + 1; i <= commitLogIndex; i++) {
                    entries.add(logs.get(i));
                }
            } finally {
                lock.unlock();
            }
        }
    }
}