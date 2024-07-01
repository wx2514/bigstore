package com.wuqing.business.bigstore.util;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.Weighers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;

/**
 * Created by wuqing on 16/2/17.
 * 拷贝对象
 */
public class CopyUtil {

    private final static Logger logger = LoggerFactory.getLogger(CopyUtil.class);

    /**
     * 缓存对象, 基于LRU算法，只保存100个对象
     */
    private static Map<String, List<Entry>> cache = new ConcurrentLinkedHashMap.Builder<String, List<Entry>>().
            maximumWeightedCapacity(100L).weigher(Weighers.singleton()).build();


    /**
     * 拷贝属性
     * @param src 数据源对象
     * @param target 目标对象
     * @param ignores 忽略的字段
     * @throws Exception
     */
    public static void copyProperty(Object src, Object target, String... ignores) {
        if (src == null || target == null) {
            return;
        }
        Set<String> ignoresSetMethods = new HashSet<>();
        for (int i = 0, k = ignores.length; i < k; i++) {
            if (ignores[i].length() == 1) {
                ignoresSetMethods.add("set" + ignores[i].substring(0, 1).toUpperCase());
            } else {
                ignoresSetMethods.add("set" + ignores[i].substring(0, 1).toUpperCase() + ignores[i].substring(1));
            }
        }
        //数据源class
        Class srcClass = src.getClass();
        //目标class
        Class targetClass = target.getClass();
        String currentMethodName = null;
        try {
            //从缓存中获取到的匹配好的对象
            List<Entry> entryList = getFromCache(srcClass, targetClass);
            for (Entry entry : entryList) {
                Object value = entry.srcMethod.invoke(src);
                if (value != null) {
                    currentMethodName = entry.targetMethod.getName();
                    if (!ignoresSetMethods.contains(currentMethodName)) {
                        entry.targetMethod.invoke(target, value);;   //如果不是忽略的字段才执行
                    }

                }
            }
        } catch (Exception e) {
            logger.error("copy data fail. src:{}, target:{}, methodName:{}", new String[] {srcClass.getName(), targetClass.getName(), currentMethodName});
            throw new IllegalArgumentException("copy data fail", e);
        }

    }

    /**
     * 从缓存中获取Method对象
     *
     * @param src
     * @param target
     * @return
     */
    private static List<Entry> getFromCache(Class src, Class target) {
        String key = src.getName() + "=" + target.getName();
        List<Entry> entryList = cache.get(key);
        if (entryList != null) {
            return entryList;
        }
        entryList = new ArrayList<Entry>();
        Method[] srcMdList = src.getMethods();
        Method[] targetMdList = target.getMethods();
        for (Method srcMd : srcMdList) {
            for (Method targetMd : targetMdList) {
                if (isMatch(srcMd, targetMd)) {
                    srcMd.setAccessible(true);  //暴力访问，避免安全验证，提高速度
                    targetMd.setAccessible(true);
                    entryList.add(new Entry(srcMd, targetMd));
                }
            }
        }
        cache.put(key, entryList);
        return entryList;
    }

    private static boolean isMatch(Method srcMd, Method targetMd) {
        if (srcMd.getParameterTypes().length != 0 || targetMd.getParameterTypes().length != 1) {
            return false;
        }
        String srcMdName = srcMd.getName();
        String targetMdName = targetMd.getName();
        String mdName = null;
        if (srcMdName.startsWith("get")) {
            mdName = srcMdName.substring(3);
        } else if (srcMdName.startsWith("is")) {
            mdName = srcMdName.substring(2);
        }
        if (mdName == null || !targetMdName.startsWith("set")) {
            return false;
        }
        if (mdName.equals(targetMdName.substring(3))) {
            return true;
        }
        return false;

    }

    /**
     * 存在缓存数据的实体对象
     */
    static class Entry {
        public Entry(Method srcMethod, Method targetMethod) {
            this.srcMethod = srcMethod;
            this.targetMethod = targetMethod;
        }
        protected Method srcMethod;
        protected Method targetMethod;
    }

    public static void main(String[] args) {
        logger.error("copy data fail. src:{}, target:{}, methodName:{}", new String[] {"srcClass", "targetClass", "currentMethodName"});
    }


}
