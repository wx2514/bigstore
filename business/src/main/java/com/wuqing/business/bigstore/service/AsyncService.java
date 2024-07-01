package com.wuqing.business.bigstore.service;

import com.wuqing.business.bigstore.config.Params;
import com.wuqing.business.bigstore.util.PoolUtil;
import com.wuqing.client.bigstore.bean.StoreBean;
import com.wuqing.client.bigstore.hold.AsyncBatchStoreDataHolder;
import com.wuqing.client.bigstore.hold.AsyncStoreDataHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 异步上报服务
 *
 */
public class AsyncService {

    private final static Logger logger = LoggerFactory.getLogger("async-store-log");

    //扫描最近时间范围内，发生变更的表
    private static final long FORCE_FLUSH_STORE_MILLISECOND = Params.getForceFlushStoreSecond() * 1000;

    private static final int FORCE_FLUSH_STORE_LINE_COUNT = Params.getForceFlushLineCount();

    private static final AsyncService MYSELF = new AsyncService();

    private LinkedBlockingQueue<AsyncStoreDataHolder> queue = new LinkedBlockingQueue(FORCE_FLUSH_STORE_LINE_COUNT * 2);



    private long lastStore = 0;

    /**
     * 执行线程
     */
    //private final Thread thread;

    /**
     * 休眠的时间，根据情况调整
     */
    private long sleepMs = 200L;

    /**
     * 线程是否执行
     */
    private boolean storeThreadExecute = true;

    public AsyncService() {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                while (storeThreadExecute) {
                    try {
                        asyncStore();
                        Thread.sleep(sleepMs);
                    } catch (Exception e) {
                        logger.error("async store fail.", e);
                    }
                }
            }
        };
        PoolUtil.CACHED_POLL.execute(runnable);
    }

    /**
     * 异步追加数据，并不会实时保存
     * 会根据策略在之后保存
     * @param storeBean
     * @return
     */
    public static boolean addStoreBean(AsyncStoreDataHolder storeBean) {
        /*boolean res = myself.queue.offer(storeBean);   //offer加入元素的时候如果队列已满则会返回false
        if (!res) {
            logger.error("queue offer fail, line:" + storeBean.toString());
        }
        return res;*/
        try {
            MYSELF.queue.put(storeBean); //改成阻塞队列了，避免丢数据
            return true;
        } catch (Exception e) {
            logger.error("add store bean fail", e);
            return false;
        }
    }

    /**
     * 批量追加数据
     * @param storeBean
     * @return
     */
    public static boolean addStoreBean(AsyncBatchStoreDataHolder storeBean) {
        try {
            for (AsyncStoreDataHolder storeData : storeBean.getDataList()) {
                MYSELF.queue.put(storeData); //改成阻塞队列了，避免丢数据
            }
            return true;
        } catch (Exception e) {
            logger.error("add store bean fail", e);
            return false;
        }
    }

    private void asyncStore() {
        int size = queue.size();
        if (size == 0) {
            return;
        }
        long now = System.currentTimeMillis();
        if (size > FORCE_FLUSH_STORE_LINE_COUNT || now - lastStore > FORCE_FLUSH_STORE_MILLISECOND) {
            logger.debug("start to flush data to bigstore");
            sleepMs = 10L;
            lastStore = now;
            Map<String, List<String[]>> map = new HashMap();
            for (int i = 0; i < size; i++) {
                AsyncStoreDataHolder storeBean = queue.poll();
                String key = storeBean.getKey();
                List<String[]> lines = map.get(key);
                if (lines == null) {
                    lines = new ArrayList<>();
                    map.put(key, lines);
                }
                lines.add(storeBean.getLine());
            }
            for (Map.Entry<String, List<String[]>> entry : map.entrySet()) {
                try {
                    String[] keys = entry.getKey().split(StoreBean.KEY_SPLIT);
                    BigstoreService.storeTable(keys[0], keys[1], entry.getValue());
                } catch (Exception e) {
                    logger.error("async store fail, key:" + entry.getKey(), e);
                }
            }
        } else {
            logger.debug("sleep to wait");
            sleepMs = 200L;
        }
    }

    /**
     * 停止刷数据到存储的服务线程
     * 并将剩余的请求刷到存储中，防止数据丢失
     */
    public static void stop() {
        try {
            MYSELF.storeThreadExecute = false;
            MYSELF.asyncStore();
        } catch (Exception e) {
            logger.error("AsyncService stop fail", e);
        }
    }

}
