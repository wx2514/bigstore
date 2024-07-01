package com.wuqing.client.bigstore.util;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by wuqing on 16/8/2.
 */
public class CommonUtil {

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    public static boolean isEmpty(Collection col) {
        return col == null || col.isEmpty();
    }

    public static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    public static boolean isEmpty(Object ob) {
        return ob == null;
    }

    public static boolean isEmpty(String[] ob) {
        return ob == null || ob.length == 0;
    }

    public static int parseInt(String s, int defaultValue) {
        if (s == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
        }
        return defaultValue;
    }

    public static int parseInt(String s) {
        if (s == null) {
            return 0;
        }
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static Integer parseInt2(String s) {
        if (s == null) {
            return null;
        }
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static long parseLong(String s) {
        if (s == null) {
            return 0;
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static long parseLong(String s, long defaultValue) {
        if (s == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static Long parseLong2(String s) {
        if (s == null) {
            return null;
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static float parseFloat(String s, float defaultValue) {
        if (s == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(s);
        } catch (NumberFormatException e) {
        }
        return defaultValue;
    }

    public static float parseFloat(String s) {
        if (s == null) {
            return 0;
        }
        try {
            return Float.parseFloat(s);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static BigDecimal parseFloat2(String s) {
        if (s == null) {
            return null;
        }
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 放大ten个10倍
     * @param ten 10的倍数
     * @return
     */
    public static float pow10(int ten) {
        long l = 1;
        for (int i = 0; i < ten; i++) {
            l = l * 10;
        }
        return l;
    }

    public static <T> List<T> asList(T... vs) {
        List<T> list = new ArrayList<T>();
        for (T v : vs) {
            list.add(v);
        }
        return list;
    }

    public static long iP2Long(String ip){
        String[] ipList = ip.split("\\.");
        if (ipList.length != 4){
            return 0L;
        }
        long ipLong = (Long.parseLong(ipList[0])<<24);
        ipLong += (Long.parseLong(ipList[1])<<16);
        ipLong += (Long.parseLong(ipList[2])<<8);
        ipLong += Long.parseLong(ipList[0]);
        return ipLong;
    }

    public static String long2Ip(long ipLong){
        return "" + String.valueOf(ipLong >>> 24) + "." +
                String.valueOf((ipLong & 0x00FFFFFF) >>> 16) + "." +
                String.valueOf((ipLong & 0x0000FFFF) >>> 8) + "." +
                String.valueOf(ipLong & 0x000000FF);
    }

    public static int byteArrayToInt(byte[] b) {
        int value= 0;
        for (int i = 0; i < 4; i++) {
            int shift= (4 - 1 - i) * 8;
            value +=(b[i] & 0x000000FF) << shift;
        }
        return value;
    }

    public static void close(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
        }
    }

    /**
     * 休眠时间
     * @param time 毫秒
     */
    public static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
        }
    }

    public static byte[] intToByteArray(final int integer) {
        int byteNum = (40 -Integer.numberOfLeadingZeros (integer < 0 ? ~integer : integer))/ 8;
        byte[] byteArray = new byte[4];

        for (int n = 0; n < byteNum; n++) {
            byteArray[3 - n] = (byte) (integer >>> (n * 8));
        }
        return byteArray;
    }

    public static <T> List<T> copyList(List<T> src) {
        if (src == null) {
            return null;
        }
        List<T> list = new ArrayList<>();
        for (T s : src) {
            list.add(s);
        }
        return list;
    }

    public static void wait(Object o, long time) {
        try {
            o.wait(time);
        } catch (InterruptedException e) {
        }
    }

    public static long round(float v) {
        long l = (long) v;
        if (v - l < 0.5) {
            return l;
        } else {
            return l + 1;
        }
    }

    public static void main(String[] args) {
        /*List<String> list = new ArrayList<>();
        System.out.println(list);
        list.add("1");
        list.add("2");
        list.add("3");
        List<String> list2 = copyList(list);
        System.out.println(list2);*/
        /*float l = pow10(1);
        System.out.println(l);*/
        String s = "123451att";
        BigDecimal b = new BigDecimal(s);
        System.out.println(b);
    }
}
