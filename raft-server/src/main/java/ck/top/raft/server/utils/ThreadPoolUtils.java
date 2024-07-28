package ck.top.raft.server.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolUtils {

    private static final Logger log = LoggerFactory.getLogger(ThreadPoolUtils.class);

    // 线程池
    private static ThreadPoolExecutor executor;
    // 线程工厂
    private static CustomThreadFactory threadFactory;
    // 异步执行结果
    private static List<CompletableFuture<Void>> completableFutures;
    // 失败数量
    private static AtomicInteger failedCount;

    static {
        failedCount = new AtomicInteger(0);
        completableFutures = new ArrayList<>();
        threadFactory = new CustomThreadFactory("通用任务执行线程池");
        executor = new ThreadPoolExecutor(4, 4, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1000), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * 执行任务
     *
     * @param runnable 任务
     */
    public static void execute(Runnable runnable) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(runnable, executor);
        // 设置好异常情况
        future.exceptionally(e -> {
            failedCount.incrementAndGet();
            log.error("Task Failed..." + e);
            e.printStackTrace();
            return null;
        });
        // 任务结果列表
        completableFutures.add(future);
    }

    public static ThreadPoolExecutor newThreadPoolExecutor(String name) {
        if (StringUtils.isEmpty(name)) {
            name = "通用任务执行线程池_" + System.currentTimeMillis();
        }
        CustomThreadFactory customThreadFactory = new CustomThreadFactory(name);
        return new ThreadPoolExecutor(4, 4, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1000), customThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ThreadPoolExecutor newFixThreadPoolExecutor(String name, Integer threadNumber) {
        if (StringUtils.isEmpty(name)) {
            name = "通用任务执行线程池_" + System.currentTimeMillis();
        }
        CustomThreadFactory customThreadFactory = new CustomThreadFactory(name);
        return new ThreadPoolExecutor(threadNumber, threadNumber, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1000), customThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * 执行自定义runnable接口（可省略，只是加了个获取taskName）
     *
     * @param runnable
     */
    public static void execute(SimpleTask runnable) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(runnable, executor);
        // 设置好异常情况
        future.exceptionally(e -> {
            failedCount.incrementAndGet();
            log.error("Task [" + runnable.taskName + "] Failed..." + e);
            e.printStackTrace();
            return null;
        });
        // 任务结果列表
        completableFutures.add(future);
    }

    /**
     * 停止线程池
     */
    public static void shutdown() {
        executor.shutdown();
        log.info("************************停止线程池************************");
        log.info("** 活动线程数：{}\t\t\t\t\t\t\t\t\t\t**", executor.getActiveCount());
        log.info("** 等待任务数：{}\t\t\t\t\t\t\t\t\t\t**", executor.getQueue().size());
        log.info("** 完成任务数：{}\t\t\t\t\t\t\t\t\t\t**", executor.getCompletedTaskCount());
        log.info("** 全部任务数：{}\t\t\t\t\t\t\t\t\t\t**", executor.getTaskCount());
        log.info("** 成功任务数：{}\t\t\t\t\t\t\t\t\t\t**", executor.getCompletedTaskCount() - failedCount.get());
        log.info("** 异常任务数：{}\t\t\t\t\t\t\t\t\t\t**", failedCount.get());
        log.info("**********************************************************");
    }

    /**
     * 线程工厂
     */
    private static class CustomThreadFactory implements ThreadFactory {
        private String poolName;
        private AtomicInteger count;

        private CustomThreadFactory(String poolName) {
            this.poolName = poolName;
            this.count = new AtomicInteger(0);
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            // 线程名，利于排查
            thread.setName(poolName + "-[线程" + count.incrementAndGet() + "]");
            return thread;
        }
    }

    /**
     * 自定义拒绝策略
     */
    private static class CustomAbortPolicy implements RejectedExecutionHandler {
        // 拒绝的任务数
        private AtomicInteger rejectCount;

        private CustomAbortPolicy() {
            this.rejectCount = new AtomicInteger(0);
        }

        private AtomicInteger getRejectCount() {
            return rejectCount;
        }

        /**
         * 这个方法，如果不抛异常，则执行此任务的线程会一直阻塞
         */
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            log.error("Task {} rejected from {} 累计：{}", r.toString(), e.toString(), rejectCount.incrementAndGet());
        }
    }

    /**
     * 只是加了个taskName，可自行实现更加复杂的逻辑
     */
    public abstract static class SimpleTask implements Runnable {
        // 任务名称
        private String taskName;

        public void setTaskName(String taskName) {
            this.taskName = taskName;
        }
    }
}
