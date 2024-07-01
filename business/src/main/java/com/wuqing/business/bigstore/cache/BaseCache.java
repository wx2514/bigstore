package com.wuqing.business.bigstore.cache;

/**
 * Created by wuqing on 17/4/3.
 */
public class BaseCache {

    public static final String SPLIT = "=";

    public static String getKey(String... ks) {
        if (ks.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (String k : ks) {
            sb.append(k).append(SPLIT);
        }
        return sb.substring(0, sb.length() - 1);
    }

}
