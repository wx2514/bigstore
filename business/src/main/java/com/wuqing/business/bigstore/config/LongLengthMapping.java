package com.wuqing.business.bigstore.config;

import java.util.Map;
import java.util.HashMap;

/**
 * Created by wuqing on 17/4/24.
 */
public class LongLengthMapping {

    /**
     * 10进制 到 64进制长度映射
     */
    private static Map<Integer, Integer> map10t64 = new HashMap<Integer, Integer>();

    /**
     * 64进制 到 10进制长度映射
     */
    private static Map<Integer, Integer> map64t10 = new HashMap<Integer, Integer>();

    static {
        map10t64.put(0, 0);
        map10t64.put(1, 1);
        map10t64.put(2, 2);
        map10t64.put(3, 2);
        map10t64.put(4, 3);
        map10t64.put(5, 3);
        map10t64.put(6, 4);
        map10t64.put(7, 4);
        map10t64.put(8, 5);
        map10t64.put(9, 5);
        map10t64.put(10, 6);
        map10t64.put(11, 7);
        map10t64.put(12, 7);
        map10t64.put(13, 8);
        map10t64.put(14, 8);
        map10t64.put(15, 9);
        map10t64.put(16, 9);
        map10t64.put(17, 10);
        map10t64.put(18, 10);
        map10t64.put(19, 11);
        map10t64.put(20, 12);

        map64t10.put(0, 0);
        map64t10.put(1, 1);
        map64t10.put(2, 2);
        map64t10.put(2, 3);
        map64t10.put(3, 4);
        map64t10.put(3, 5);
        map64t10.put(4, 6);
        map64t10.put(4, 7);
        map64t10.put(5, 8);
        map64t10.put(5, 9);
        map64t10.put(6, 10);
        map64t10.put(7, 11);
        map64t10.put(7, 12);
        map64t10.put(8, 13);
        map64t10.put(8, 14);
        map64t10.put(9, 15);
        map64t10.put(9, 16);
        map64t10.put(10, 17);
        map64t10.put(10, 18);
        map64t10.put(11, 19);
        map64t10.put(11, 19);
        map64t10.put(12, 20);
    }

    public static int get64By10(int i) {
        Integer res = map10t64.get(i);
        return res == null ? 0 : res;
    }

    public static int get10By64(int i) {
        Integer res = map64t10.get(i);
        return res == null ? 0 : res;
    }

}
