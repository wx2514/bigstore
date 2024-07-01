package com.wuqing.business.bigstore.util;

import com.wuqing.business.bigstore.config.Params;

import java.util.concurrent.*;

/**
 * Created by wuqing on 17/3/9.
 */
public class PoolUtil {

    /**
     *  等待队列10万个任务
     */
    private static final int BLOCKING_QUEUE_SIZE = 100000;

    public static final ExecutorService NETTY_FIX_POLL = newFixedThreadPool(Params.getNettyThreadSize(), "bigstore-netty-server");

    public static final ExecutorService NETTY_SYNC_FILE_FIX_POLL = newFixedThreadPool(5, "bigstore-netty-sync-file");

    public static final ExecutorService QUERY_FIX_POLL_QUERY = newFixedThreadPool(Params.getQueryThreadSize(), "bigstore-query-query");

    public static final ExecutorService QUERY_FIX_POLL_SEARCH = newFixedThreadPool(Params.getQueryThreadSize(), "bigstore-query-search");

    public static final ExecutorService QUERY_FIX_POLL_GETDATA = newFixedThreadPool(Params.getQueryThreadSize(), "bigstore-query-getdata");

    public static final ExecutorService WRITE_FIX_POLL = newFixedThreadPool(Params.getWriteThreadSize(), "bigstore-write");

    public static final ExecutorService COMPRESS_FIX_POLL = newFixedThreadPool(Params.getCompressThreadSize(), "bigstore-compress");

    public static final ExecutorService GROUP_QUERY_POLL = newFixedThreadPool(Params.getNettyThreadSize(), "bigstore-group-query");

    public static final ExecutorService CACHED_POLL = newCachedThreadPool("cached-live-forever");

    /**
     * 压缩信号量，避免放入线程池的速率远远大于处理速率，导致的积压。目前是压缩线程池数量的2倍
     */
    public static final Semaphore COMPRESS_SEMAP = new Semaphore(Params.getCompressThreadSize() * 2);

    /**
     * 俩个rsync同步线程
     */
    public static final ExecutorService SEND_FIX_POLL = newFixedThreadPool(2, "bigstore-send");

    public static final ExecutorService newFixedThreadPool(int nThreads, String poolName) {
        return new ThreadPoolExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(BLOCKING_QUEUE_SIZE),
                new StoreThreadFactory(poolName));
    }

    public static final ExecutorService newCachedThreadPool(String poolName) {
        return new ThreadPoolExecutor(0, 10000, 60L,
                TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new StoreThreadFactory(poolName));
    }

}
