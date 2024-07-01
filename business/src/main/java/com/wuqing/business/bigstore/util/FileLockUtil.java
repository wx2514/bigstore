package com.wuqing.business.bigstore.util;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by wuqing on 17/4/3.
 */
public class FileLockUtil {

    private final static Logger logger = LoggerFactory.getLogger(FileLockUtil.class);

    /**
     * 缓存1万个文件锁
     */
    private static Map<String, ReentrantLock> fileLock = new ConcurrentLinkedHashMap.Builder<String, ReentrantLock>().maximumWeightedCapacity(30000)
            .listener(new EvictionListener<String, ReentrantLock>() {
                @Override
                public void onEviction(String key, ReentrantLock lock) {  //添加监听事件，当对象被释放后，主动释放内存，而不是等待full gc后再释放
                    try {
                        if (lock.isLocked()) {
                            lock.unlock();
                        }
                    } catch (Exception e) {
                        logger.error("when lock is out to cache, unlock fail", e);
                    }
                }
            }).build();

    /*public static ReentrantLock lock(String path) {
        //System.out.println("lock:" + dataBase + "=" + table + ", thread:" + Thread.currentThread().getId());
        ReentrantLock lock = getLock(path);
        lock.lock();
        return lock;
    }*/

    /*public static void unlock(String key) {
        //System.out.println("unlock:" + dataBase + "=" + table + ", thread:" + Thread.currentThread().getId());
        getLock(key).unlock();
    }*/

    public static synchronized ReentrantLock getLock(String key) {
        ReentrantLock lock = fileLock.get(key);
        if (lock == null) {
            lock = new ReentrantLock();
            fileLock.put(key, lock);
        }
        return lock;
    }

}
