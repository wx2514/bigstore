package com.wuqing.business.bigstore.cache;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.wuqing.business.bigstore.config.Params;
import com.wuqing.business.bigstore.util.BlockLockUtil;
import com.wuqing.business.bigstore.util.PoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by wuqing on 17/3/3.
 * 数据块缓存
 * 使用堆外内存
 */
public class DataCache extends BaseCache {

    private final static Logger logger = LoggerFactory.getLogger("cache-log");

    private final static int SLEEP_SECOND = 10;

    private static int executeCount = 0;    //每3次执行一次操作（30秒）

    /**
     * 缓存获取direckMemory的Max的字段
     */
    private static Field maxMemory = null;

    /**
     * 缓存获取direckMemory的Used的字段
     */
    private static Field reservedMemory = null;

    static {
        try {
            Class<?> c = Class.forName("java.nio.Bits");
            maxMemory = c.getDeclaredField("maxMemory");
            maxMemory.setAccessible(true);
            reservedMemory = c.getDeclaredField("reservedMemory");
            reservedMemory.setAccessible(true);
        } catch (Exception e) {
            logger.error("invoke static get directMemory field fail", e);
        }
    }

    private static ConcurrentLinkedHashMap<String, ByteBuffer> map = new ConcurrentLinkedHashMap.Builder<String, ByteBuffer>()
            .maximumWeightedCapacity(Params.getDataCacheSize()).listener(new EvictionListener<String, ByteBuffer>() {
                @Override
                public void onEviction(String key, ByteBuffer value) {  //添加监听事件，当对象被释放后，主动释放内存，而不是等待full gc后再释放
                    ReentrantLock lock = BlockLockUtil.getLock(key);
                    lock.lock();
                    try {   //如果正在使用中，这里会卡一下，保证不会回收
                        DirectBufferClean.clean(value);
                    } finally {
                        lock.unlock();
                    }
                }
            }).build();    //缓存1000个块内存

    /**
     * 将数据存储到堆外缓存
     * @param dataBase
     * @param table
     * @param dir
     * @param col
     * @param bytes
     */
    public static void put(String dataBase, String table, String dir, String col, byte[] bytes) {
        if (bytes == null) {
            return;
        }
        String key = dataBase + SPLIT + table + SPLIT + dir + SPLIT + col;
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        map.put(key, buffer);
    }

    /**
     * 获取缓存数据
     * @param dataBase
     * @param table
     * @param dir
     * @param col
     * @return
     */
    public static byte[] get(String dataBase, String table, String dir, String col) {
        String key = dataBase + SPLIT + table + SPLIT + dir + SPLIT + col;
        byte[] bytes = null;
        ReentrantLock lock = BlockLockUtil.getLock(key);
        lock.lock();
        try {
            ByteBuffer buffer = map.get(key);
            if (buffer == null) {
                return null;
            }
            bytes = new byte[buffer.limit()];
            buffer.get(bytes);
            //buffer.position(0);
            buffer.clear();
        } catch (Throwable e) {
            logger.error("get data cache fail", e);
            bytes = null;
        } finally {
            lock.unlock();
        }
        return bytes;
    }

    /**
     * 将数据从堆外内存中移除
     * @param dataBase
     * @param table
     * @param dir
     * @param col
     * @throws Exception
     */
    public static void remove(String dataBase, String table, String dir, String col) throws Exception {
        String key = dataBase + SPLIT + table + SPLIT + dir + SPLIT + col;
        ByteBuffer bf = map.remove(key);
        ReentrantLock lock = BlockLockUtil.getLock(key);;
        lock.lock();
        try {   //如果正在使用中，这里会卡一下，保证不会回收

        } finally {
            lock.unlock();
        }
        if (bf != null ) {    //释放直接内存
            DirectBufferClean.clean(bf);
        }

    }

    /**
     * 在自动清理space时，也清理缓存
     * @param dataBase
     * @param table
     * @param dirs
     */
    public static void clearByClean(String dataBase, String table, List<String> dirs) {
        List<String> pres = new ArrayList<>();
        for (String dir : dirs) {
            String pre = getKey(dataBase, table, dir);
            pres.add(pre);
        }
        for (Iterator<Map.Entry<String, ByteBuffer>> it = map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, ByteBuffer> entry = it.next();
            for (String pre : pres) {
                if (entry.getKey().startsWith(pre)) {
                    it.remove();
                    break;
                }
            }

        }
    }

    /**
     * 扫描并监控directMemory，只能调整数据缓存队列长度
     */
    public static void directCheck() {
        Runnable run = new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        long max = 0L;
                        long reserved = 0L;
                        Object maxOb =  maxMemory.get(null);
                        if (maxOb instanceof Long) {
                            max = (Long) maxOb;
                        } else if (maxOb instanceof AtomicLong) {
                            max = ((AtomicLong) maxOb).longValue();
                        } else {
                            max = Long.parseLong(maxOb.toString());
                        }
                        Object reservedOb = reservedMemory.get(null);
                        if (reservedOb instanceof Long) {
                            reserved = (Long) reservedOb;
                        } else if (reservedOb instanceof AtomicLong) {
                            reserved = ((AtomicLong) reservedOb).longValue();
                        } else {
                            reserved = Long.parseLong(reservedOb.toString());
                        }
                        long maxSize = map.capacity();
                        int usedSize = map.size();
                        logger.info("maxMemory     :" + max);
                        logger.info("reservedMemory:" + reserved);
                        logger.info("maxSize :" + maxSize);
                        logger.info("usedSize:" + usedSize);
                        logger.info("tableCache:" + TableCache.sizeOfTable());
                        logger.info("spaceCache:" + TableCache.sizeOfSpace());
                        logger.info("indexCache:" + TableCache.sizeOfIndex());
                        logger.info("enumCache:" + TableCache.sizeOfEnum());
                        logger.info("enumIndexCache:" + TableCache.sizeOfIndexEnum());


                        int percent = (int) (reserved * 100 / max);
                        if (percent > 75) {
                            map.setCapacity((long) (usedSize * 0.95));
                        } else if (percent < 70 && usedSize == maxSize) {
                            map.setCapacity((long) (usedSize * 1.02));
                        }
                        if (map.capacity() < 100) {
                            logger.warn("capacity is less than 100, so to 100");
                            System.gc();    //如果容量已经缩减到100以内的了，说明可能有大量的堆外内存没有被回收，所以执行GC进行回收
                            map.setCapacity(100);
                        }
                        if (Params.isSlave() && ++executeCount > 3) { //如果是从库定时删除，缓存中的table缓存
                            executeCount = 0;
                            TableCache.TABLE_MAP.clear();
                            TableCache.SPACE_MAP.clear();
                            TableCache.ENUM_MAP.clear();
                        }
                        Thread.sleep(SLEEP_SECOND * 1000L);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        PoolUtil.CACHED_POLL.execute(run);
    }

    public static void main(String[] args) {

    }

}
