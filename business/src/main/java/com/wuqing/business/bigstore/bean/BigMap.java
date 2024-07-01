package com.wuqing.business.bigstore.bean;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import java.io.Serializable;
import java.util.*;

/**
 * Created by wuqing on 17/8/29.
 * 目前ConcurrentLinkedHashMap在百万级性能还是不错的，没必要分段
 */
@Deprecated
public class BigMap<K,V> implements Map<K,V>, Serializable {

    private static final long serialVersionUID = 1L;

    private static final int LENGTH = 10;
    /**
     * 每个存储快索引缓存
     */
    private ConcurrentLinkedHashMap<K, V>[] indexMap = new ConcurrentLinkedHashMap[LENGTH];

    public BigMap(int size) {   //初始化
        int each = size / 10;
        for (int i = 0, k = indexMap.length; i < k; i++) {
            indexMap[i] = new ConcurrentLinkedHashMap.Builder<K,V>().maximumWeightedCapacity(each).build();    //缓存10000个快索引信息
        }
    }

    @Override
    public V put(K key, V value) {
        if (key == null || value == null) {
            return null;
        }
        return indexMap[getIndex(key)].put(key, value);
    }

    @Override
    public V get(Object key) {
        if (key == null) {
            return null;
        }
        return indexMap[getIndex(key)].get(key);
    }

    @Override
    public V remove(Object key) {
        if (key == null) {
            return null;
        }
        return indexMap[getIndex(key)].remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            this.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        for (int i = 0, k = indexMap.length; i < k; i++) {
            indexMap[i].clear();
        }
    }

    @Override
    public Set<K> keySet() {
        Set<K> set = new HashSet<K>();
        for (int i = 0, k = indexMap.length; i < k; i++) {
            set.addAll(indexMap[i].keySet());
        }
        return set;
    }

    @Override
    public Collection<V> values() {
        List<V> values = new ArrayList<V>();
        for (int i = 0, k = indexMap.length; i < k; i++) {
            values.addAll(indexMap[i].values());
        }
        return values;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        Set<Entry<K, V>> set = new HashSet<Entry<K, V>>();
        for (int i = 0, k = indexMap.length; i < k; i++) {
            set.addAll(indexMap[i].entrySet());
        }
        return set;
    }

    @Override
    public int size() {
        int size = 0;
        for (ConcurrentLinkedHashMap<K, V> map : indexMap) {
            size += map.size();
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        for (ConcurrentLinkedHashMap<K, V> map : indexMap) {
            if (!map.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean containsKey(Object key) {
        return indexMap[getIndex(key)].containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        for (ConcurrentLinkedHashMap<K, V> map : indexMap) {
            if (map.containsValue(value)) {
                return true;
            }
        }
        return false;
    }

    private int getIndex(Object key) {
        return (key.hashCode() & 0x7FFFFFFF) % LENGTH;
    }

}
