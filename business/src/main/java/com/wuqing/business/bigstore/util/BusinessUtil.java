package com.wuqing.business.bigstore.util;

import com.wuqing.business.bigstore.config.Params;

/**
 * Created by wuqing on 18/5/23.
 */
public class BusinessUtil {

    private static int HASHCODE_RETAIN = Params.getHashcodeRetainSize();

    public static String getHashCodeStr(String s) {
        String str = Convert10To64.compressNumber(Math.abs(s.hashCode()));
        int length = str.length();
        if (length > HASHCODE_RETAIN) {
            return str.substring(str.length() - HASHCODE_RETAIN);
        }
        for (int i = 0, k = HASHCODE_RETAIN - length; i < k; i++) {
            str = "0" + str;
        }
        return str;
    }

    public static void main(String[] args) {
        System.out.println(getHashCodeStr("12"));
    }

}
