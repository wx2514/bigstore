package com.wuqing.business.bigstore.cache;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.wuqing.client.bigstore.bean.FunctionData;
import com.wuqing.business.bigstore.util.KryoServerUtil;
import com.wuqing.business.bigstore.util.PoolUtil;
import com.wuqing.client.bigstore.bean.QueryResult;
import com.wuqing.business.bigstore.config.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wuqing on 17/3/10.
 * 查询缓存
 */
public class QueryCache extends BaseCache {

    /**
     * 上一次GC的时间
     */
    private static long lastGc = 0;

    private final static Logger cacheLogger = LoggerFactory.getLogger("cache-log");

    private static final int WAIT_OUT = 60 * 1000;  //当缓存结果超过 3 分钟都没被使用时，被移除

    private static final int TIME_OUT = 3 * 60 * 1000;  //当缓存结果超过 3 分钟都没被使用时，被移除

    private static final QueryCache SINTOL = new QueryCache();

    private static AtomicInteger TOTAL = new AtomicInteger(0);

    private static AtomicInteger USED_CACHE = new AtomicInteger(0);

    private static Map<String, Cache> map = new ConcurrentLinkedHashMap.Builder<String, Cache>()
            .maximumWeightedCapacity(Params.getQueryCacheSize()).listener(new EvictionListener<String, Cache>() {
                @Override
                public void onEviction(String s, Cache cache) {
                    ByteBuffer buffer = cache.data;
                    if (buffer == null) {
                        return;
                    }
                    synchronized (buffer) {
                        DirectBufferClean.clean(buffer);
                    }
                }
            }).build();    //缓存10000个块内存

    private QueryCache() {
        Runnable run = new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        for (Iterator<Map.Entry<String, Cache>> it = map.entrySet().iterator(); it.hasNext();) {
                            Map.Entry<String, Cache> entry = it.next();
                            if (entry.getValue() == null || (System.currentTimeMillis() - entry.getValue().created > TIME_OUT)) {
                                it.remove();
                                //cacheLogger.debug("remove cache, created:" + entry.getValue().created);
                            }
                        }
                        cacheLogger.debug("cache size:" + map.size());
                        int percent = TOTAL.get() == 0 ? 0 : USED_CACHE.get() * 100 / TOTAL.get();
                        cacheLogger.debug("cache percent:" + percent);
                        TOTAL.set(0);
                        USED_CACHE.set(0);

                        Calendar c = Calendar.getInstance();
                        int i = c.get(Calendar.HOUR_OF_DAY);
                        long now = System.currentTimeMillis();
                        if (i == 2 && (now - lastGc > 86400000)) {  //凌晨2点，并且距离上一次GC大于一天，就手动执行一次GC
                            cacheLogger.info("Execute GC");
                            System.gc();
                            lastGc = now;
                        }

                        Thread.sleep(WAIT_OUT);
                    } catch (Exception e) {
                        cacheLogger.error("clean query cache fail.", e);
                    }
                }
            }
        };
        PoolUtil.CACHED_POLL.execute(run);
    }

    //@Deprecated
    public static Map<String, List<FunctionData>> getSecondAggregationData(String countKey, String dir, String queryAggType) {
        String key = dir + SPLIT + countKey + SPLIT + queryAggType + SPLIT + "$secondaggregation$";
        return (Map<String, List<FunctionData>>) get(key);
    }

    //@Deprecated
    public static void putSecondAggregationData(String countKey, String dir, String queryAggType, Map<String, List<FunctionData>> data) {
        String key = dir + SPLIT + countKey + SPLIT + queryAggType + SPLIT + "$secondaggregation$";
        put(key, data);
    }

    public static void putColData(String countKey, String dir, List<String> data) {
        String key = dir + SPLIT + countKey + SPLIT + "$coldata$";
        put(key, data);
    }

    public static List<String> getColData(String countKey, String dir) {
        String key = dir + SPLIT + countKey + SPLIT + "$coldata$";
        return (List<String>) get(key);
    }


    public static QueryResult get(String countKey, String dir) {
        String key = dir + SPLIT + countKey;
        return (QueryResult) get(key);
    }

    public static Object get(String key) {
        TOTAL.incrementAndGet();
        Cache cache =  map.get(key);
        if (cache == null) {
            return null;
        }
        long now = System.currentTimeMillis();
        if (now - cache.created > TIME_OUT) {
            map.remove(key);
            return null;
        }
        cache.created = now;    //重置时间。
        USED_CACHE.incrementAndGet();
        ByteBuffer buffer = cache.data;
        if (buffer == null) {
            return null;
        }
        Object result = null;
        try {
            synchronized (buffer) {
                byte[] bytes = new byte[buffer.limit()];
                buffer.get(bytes);
                //buffer.position(0);
                buffer.clear();
                result = KryoServerUtil.readFromByteArray(bytes);
            }
        } catch (Throwable e) {
            cacheLogger.error("get query cache fail", e);
            result = null;
        }
        return result;
    }

    public static void put(String countKey, String dir, QueryResult data) {
        String key = dir + SPLIT + countKey;
        put(key, data);
    }

    public static void put(String key, Object data) {
        if (data == null) {
            return;
        }
        map.put(key, new Cache(System.currentTimeMillis(), data));
    }

    public static void clear(String dataBase, String table, String col, String dir) {
        String pre = null;
        if (col == null) {
            pre =  getKey(dir, dataBase, table);
        } else {
            pre = getKey(dir, dataBase, table, col);
        }
        for (Iterator<Map.Entry<String, Cache>> it = map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Cache> entry = it.next();
            if (entry.getKey().startsWith(pre)) {
                it.remove();
            }
        }
    }

    /**
     * 暂时先不清理吧，如果后续发现 在clean过程中，确实出现问题，再清理
     * @param dataBase
     * @param table
     * @param dirs
     */
    public static void clearByClean(String dataBase, String table, List<String> dirs) {
        List<String> pres = new ArrayList<>();
        for (String dir : dirs) {
            String pre = getKey(dir, dataBase, table);
            pres.add(pre);
        }
        for (Iterator<Map.Entry<String, Cache>> it = map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Cache> entry = it.next();
            for (String pre : pres) {
                if (entry.getKey().startsWith(pre)) {
                    it.remove();
                    break;
                }
            }

        }
    }

    public static class Cache {
        private long created;
        private ByteBuffer data;

        public Cache(long created, Object data) {
            this.created = created;
            byte[] bytes = KryoServerUtil.writeToByteArray(data);
            ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
            buffer.put(bytes);
            buffer.flip();
            this.data = buffer;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Thread.sleep(600 * 1000);
    }

}
