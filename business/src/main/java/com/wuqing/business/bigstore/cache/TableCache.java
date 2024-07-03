package com.wuqing.business.bigstore.cache;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.wuqing.business.bigstore.config.Params;
import com.wuqing.business.bigstore.util.FileUtil;
import com.wuqing.client.bigstore.bean.EnumInfo;
import com.wuqing.business.bigstore.bean.IndexInfo;
import com.wuqing.business.bigstore.bean.SpaceInfo;
import com.wuqing.business.bigstore.bean.TableInfo;

import java.util.List;

/**
 * Created by wuqing on 17/3/9.
 * 表索引缓存
 */
public class TableCache extends BaseCache {

    /**
     * 每个存储快索引缓存
     */
    private static final ConcurrentLinkedHashMap<String, IndexInfo> INDEX_MAP = new ConcurrentLinkedHashMap.Builder<String, IndexInfo>()
            .maximumWeightedCapacity(Params.getIndexCacheSize()).build();    //缓存10000个快索引信息

    private static final ConcurrentLinkedHashMap<String, IndexInfo> INDEX_EM_MAP = new ConcurrentLinkedHashMap.Builder<String, IndexInfo>()
            .maximumWeightedCapacity(Params.getIndexEnumCacheSize()).build();    //缓存10000个快索引信息

    //private static final Map<String, IndexInfo> indexMap = new BigMap<String, IndexInfo>(Params.getIndexCacheSize());    //缓存10000个快索引信息

    /**
     * 表信息缓存
     */
    protected static final ConcurrentLinkedHashMap<String, TableInfo> TABLE_MAP = new ConcurrentLinkedHashMap.Builder<String, TableInfo>()
            .maximumWeightedCapacity(Params.getTableCacheSize()).build();    //缓存1000张表信息

    /**
     * 表分区信息缓存
     */
    protected static final ConcurrentLinkedHashMap<String, SpaceInfo> SPACE_MAP = new ConcurrentLinkedHashMap.Builder<String, SpaceInfo>()
            .maximumWeightedCapacity(Params.getSpaceCacheSize()).build();    //缓存1000分区信息

    /**
     * 列枚举信息缓存
     */
    protected static final ConcurrentLinkedHashMap<String, EnumInfo> ENUM_MAP = new ConcurrentLinkedHashMap.Builder<String, EnumInfo>()
            .maximumWeightedCapacity(Params.getEnumCacheSize()).build();    //缓存1000个枚举列信息

    /**
     * 从缓存中获取
     * @return
     */
    public static IndexInfo getIndex(String dataBase, String table, String dir, String col) {
        String key = getKey(dataBase, table, dir, col);
        IndexInfo info = INDEX_EM_MAP.get(key);
        if (info != null) { //先从 indexEMMap 中获取，再从 indexMap 中获取
            return info;
        }
        return INDEX_MAP.get(key);
    }

    /**
     * 添加缓存数据
     * @param indexInfo
     */
    public static void putIndex(String dataBase, String table, String dir, String col, IndexInfo indexInfo) {
        if (indexInfo == null) {
            return;
        }
        String key = getKey(dataBase, table, dir, col);
        if (indexInfo.existEnumIndexes() || indexInfo.existKeywordIndexes()) {  //枚举索引 和 关键词索引 都 放入枚举索引map中 缓存
            INDEX_EM_MAP.put(key, indexInfo);
        } else {
            INDEX_MAP.put(key, indexInfo);
        }

    }

    /**
     * 从缓存中获取表信息
     * @param dataBase
     * @param table
     * @return
     */
    public static TableInfo getTableInfo(String dataBase, String table) {
        return TABLE_MAP.get(getKey(dataBase, table));
    }

    /**
     * 将表信息放入缓存
     * @param dataBase
     * @param table
     * @param tbInfo
     */
    public static void putTableInfo(String dataBase, String table, TableInfo tbInfo) {
        if (tbInfo == null) {
            return;
        }
        TABLE_MAP.put(getKey(dataBase, table), tbInfo);
    }

    /**
     * 将spaceInfo放入缓存
     * @param dataBase
     * @param table
     * @param space
     * @param col
     * @param spaceInfo
     */
    public static void putSpaceInfo(String dataBase, String table, String space, String col, SpaceInfo spaceInfo) {
        if (spaceInfo == null) {
            return;
        }
        String key = getKey(dataBase, table, space, col);
        SPACE_MAP.put(key, spaceInfo);
    }

    /**
     * 根据key获取表分区空间信息
     * @param dataBase
     * @param table
     * @param space
     * @param col
     * @return
     */
    public static SpaceInfo getSpaceInfo(String dataBase, String table, String space, String col) {
        String key = getKey(dataBase, table, space, col);
        return SPACE_MAP.get(key);
    }

    public static EnumInfo getEnumInfo(String dataBase, String table, String col) {
        String key = getKey(dataBase, table, col);
        return ENUM_MAP.get(key);
    }

    public static EnumInfo putEnumInfo(String dataBase, String table, String col, EnumInfo enumInfo) {
        String key = getKey(dataBase, table, col);
        return ENUM_MAP.put(key, enumInfo);
    }

    public static EnumInfo removeEnumInfo(String dataBase, String table, String col) {
        String key = getKey(dataBase, table, col);
        return ENUM_MAP.remove(key);
    }

    /**
     * 将索引缓存清除
     */
    public static void removeIndexCache(String dataBase, String table, String dir, String col) {
        String key = getKey(dataBase, table, dir, col);
        INDEX_EM_MAP.remove(key);
        INDEX_MAP.remove(key);
    }

    /**
     * 将表信息缓存清除
     */
    public static void removeTableCache(String dataBase, String table) {
        TABLE_MAP.remove(getKey(dataBase, table));
    }

    /**
     * 将表分区缓存清除
     * @param dataBase
     * @param table
     * @param space
     * @param col
     */
    public static void removeSpaceCache(String dataBase, String table, String space, String col) {
        String key = getKey(dataBase, table, space, col);
        SPACE_MAP.remove(key);
    }

    public static void clearTableCahce() {
        TABLE_MAP.clear();
    }

    public static void main(String[] args) throws Exception {
        List<String> list = FileUtil.readAll("/tmp/bigstore/default_data_base/test_table/rows/0000000000/0000000035/time_desc.txt", false);
        long s = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            IndexInfo indexInfo = new IndexInfo(list);
            INDEX_MAP.put("lurker-table-dir-dfdfsfaf-1" + i, indexInfo);
            if (i % 100000 == 0) {
                long time = System.currentTimeMillis() - s;
                s = System.currentTimeMillis();
                System.out.println(INDEX_MAP.size() + ":" + time);
            }
        }
        System.out.println(INDEX_MAP.size());
        long nao = System.nanoTime();
        INDEX_MAP.get("lurker-table-dir-dfdfsfaf-1" + 10000);
        System.out.println("get-time:" + (System.nanoTime() - nao));
    }

    /**
     * 表缓存使用情况
     * @return
     */
    public static String sizeOfTable() {
        return TABLE_MAP.size() + "/" + TABLE_MAP.capacity();
    }

    /**
     * 表分区缓存使用情况
     * @return
     */
    public static String sizeOfSpace() {
        return SPACE_MAP.size() + "/" + SPACE_MAP.capacity();
    }

    /**
     * DP索引缓存使用情况
     * @return
     */
    public static String sizeOfIndex() {
        return INDEX_MAP.size() + "/" + INDEX_MAP.capacity();
    }

    /**
     * 枚举缓存使用情况
     * @return
     */
    public static String sizeOfEnum() {
        return ENUM_MAP.size() + "/" + ENUM_MAP.capacity();
    }

    /**
     * 枚举索引缓存使用情况
     * @return
     */
    public static String sizeOfIndexEnum() {
        return INDEX_EM_MAP.size() + "/" + INDEX_EM_MAP.capacity();
    }



}
