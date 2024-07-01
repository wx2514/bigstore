package com.wuqing.client.bigstore.util;

import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * Created by wuqing on 17/3/8.
 * 实验了下，怎么感觉还没有GZIP快，，悲剧啊，WHY
 * 最后发现是初始化慢一些
 */
//@Deprecated
public class SnappyUtil {

    public static byte[] compress(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return Snappy.compress(bytes);
        } catch (Throwable e) {
            //e.printStackTrace();
            return null;
        }
    }

    public static byte[] decompressWithCheck(byte[] bytes) {
        if (isValid(bytes)) {
            return decompress(bytes);
        }
        return null;
    }

    private static boolean isValid(byte[] bytes) {
        if (bytes == null) {
            return false;
        }
        try {
            return Snappy.isValidCompressedBuffer(bytes);
        } catch (IOException e) {
            return false;
        }
    }

    public static byte[] decompress(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return Snappy.uncompress(bytes);
        } catch (IOException e) {
            //e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        String sss = "^,6@,4\\,38,9H,38,38,9H,1T,38,1T,38,1T,1T,1T,6@,1T,1T,1T,7d,1T,7d,38,38,7d,6@,1T,1T,1T,:l,7d,1T,1T,1T,6@,6@,1T,1T,1T,1T,38,1T,1T,1T,1T,4\\,4\\,1T,1T,38,1T,4\\,1T,1T,1T,38,1T,1T,1T,1T,38,1T,4\\,1T,38,1T,1T,1T,38,:l,1T,38,6@,38,1T,4\\,:l,6@,1T,1T,4\\,1T,:l,38,38,1T,38,6@,1T,1T,38,38,38,1T,38,38,6@,1T,1T,1T,4\\,1T,4\\,38,38,1T,1T,1T,38,1T,9H,38,4\\,1T,1T,38,1T,1T,1T,1T,1T,38,1T,1T,1T,1T,1T,38,7d,38,4\\,1T,4\\,1T,38,1T,4\\,38,38,1T,38,38,1T,1T,1T,1T,1T,1T,38,1T,1T,1T,6@,4\\,1T,38,1T,1T,1T,1T,1T,38,38,1T,1T,1T,6@,1T,9H,1T,4\\,38,6@,4\\,1T,38,38,1T,1T,1T,38,1T,1T,38,1T,7d,38,4\\,38,1T,1T,1T,4\\,4\\,38,4\\,38,38,38,38,1T,1T,7d,1T,38,1T,1T,38,38,6@,1T,38,1T,1T,4\\,38,38,1T,4\\,4\\,4\\,4\\,38,1T,38,4\\,1T,1T,1T,1T,38,38,1T,1T,4\\,38,4\\,1T,38,1T,1T,4\\,38,1T,4\\,1T,38,:l,6@,1T,4\\,38,38,1T,1T,1T,1T,1T,38,1T,4\\,4\\,38,1T,1T,1T,1T,4\\,6@,4\\,1T,38,1T,1T,1T,1T,38,1T,7d,4\\,1T,38,1T,1T,1T,38,1T,4\\,1T,1T,1T,6@,1T,1T,4\\,4\\,1T,4\\,38,1T,1T,1T,1T,9H,1T,1T,4\\,38,38,1T,1T,6@,1T,38,7d,38,38,1T,38,38,38,1T,1T,1T,1T,1T,1T,1T,1T,1T,1T,1T,4\\,1T,38,7d,6@,1T,38,1T,38,1T,1T,1T,1T,1T,7d,4\\,1T,38,1T,1T,1T,4\\,38,7d,1T,1T,1T,38,1T,4\\,4\\,38,38,38,38,38,38,<P,4\\,4\\,38,38,38,1T,1T,1T,1T,38,1T,1T,38,1T,1T,1T,38,1T,38,1T,38,1T,38,1T,1T,7d,1T,1T,1T,38,1T,1T,1T,1T,38,1T,4\\,38,9H,1T,1T,4\\,1T,1T,7d,6@,1T,1T,1T,38,6@,1T,1T,4\\,38,38,1T,1T,1T,38,4\\,1T,1T,1T,38,1T,1T,6@,38,1T,7d,38,4\\,9H,1T,1T,1T,1T,1T,1T,38,4\\,6@,:l,7d,1T,38,38,1T,4\\,38,9H,1T,1T,1T,1T,1T,1T,1T,1T,1T,1T,<P,38,6@,1T,1T,1T,38,4\\,>4,1T,1T,6@,1T,1T,1T,6@,1T,7d,1T,4\\,38,1T,1T,1T,4\\,1T,1T,1T,1T,?X,1T,1T,1T,1T,38,1T,1T,1T,1T,1T";
        byte[] bs = compress(sss.getBytes());
        byte[] ff = SnappyUtil.decompress(SnappyUtil.compress(bs));
        System.out.println(ff);
    }

}
