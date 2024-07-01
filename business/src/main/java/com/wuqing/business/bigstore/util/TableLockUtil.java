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
public class TableLockUtil {

    private final static Logger logger = LoggerFactory.getLogger(TableLockUtil.class);

    //private static Map<String, ReentrantLock> tableLock = new ConcurrentHashMap<String, ReentrantLock>();

    /**
     * 缓存1000个表锁
     */
    private static Map<String, ReentrantLock> tableLock = new ConcurrentLinkedHashMap.Builder<String, ReentrantLock>().maximumWeightedCapacity(1000)
            .listener(new EvictionListener<String, ReentrantLock>() {
                @Override
                public void onEviction(String key, ReentrantLock lock) {  //添加监听事件，当对象被释放后，主动释放内存，而不是等待full gc后再释放
                    try {
                        lock.unlock();
                    } catch (Exception e) {
                        //logger.error("when lock is out to cache, unlock fail", e);   //从缓存中被移出去的话, 这个lock已经被unlock的概率也很大
                    }
                }
            }).build();

    /*public static ReentrantLock lock(String dataBase, String table) {
        //System.out.println("lock:" + dataBase + "=" + table + ", thread:" + Thread.currentThread().getId());
        ReentrantLock lock = getLock(dataBase, table);
        lock.lock();
        return lock;
    }*/

    /*public static void unlock(String dataBase, String table) {
        //System.out.println("unlock:" + dataBase + "=" + table + ", thread:" + Thread.currentThread().getId());
        getLock(dataBase, table).unlock();
    }*/

    public static synchronized ReentrantLock getLock(String dataBase, String table) {
        String key = dataBase + "=" + table;
        ReentrantLock lock = tableLock.get(key);
        if (lock == null) {
            lock = new ReentrantLock();
            tableLock.put(key, lock);
        }
        return lock;
    }

}
