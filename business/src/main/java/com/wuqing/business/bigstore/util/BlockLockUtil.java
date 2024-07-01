package com.wuqing.business.bigstore.util;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by wuqing on 17/6/19.
 */
public class BlockLockUtil {

    private final static Logger logger = LoggerFactory.getLogger(BlockLockUtil.class);

    private static final int CACHE_SIZE = 1000;

    private static Map<String, ReentrantLock> blockLock = new ConcurrentLinkedHashMap.Builder<String, ReentrantLock>()
            .maximumWeightedCapacity(CACHE_SIZE).build();

    public static synchronized ReentrantLock getLock(String key) {
        ReentrantLock lock = blockLock.get(key);
        if (lock == null) {
            lock = new ReentrantLock();
            blockLock.put(key, lock);
        }
        return lock;
    }

    /**
     * 加锁
     * @return
     */
    /*public static ReentrantLock lock(String key) {
        ReentrantLock lock = getLock(key);
        lock.lock();
        return lock;
    }*/

    //unlock 外围自行调用

}
