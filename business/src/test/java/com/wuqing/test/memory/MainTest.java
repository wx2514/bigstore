package com.wuqing.test.memory;

import java.io.UnsupportedEncodingException;

/**
 * Created by wuqing on 17/4/3.
 */
public class MainTest {

    public static void main(String[] args) throws UnsupportedEncodingException {
        String s = "sdfdsfsdfsf";
        byte[] bt =  s.getBytes();
        String sss = new String(bt, "utf-8");
        sss = new String(bt, "utf-8");
        sss = new String(bt, "utf-8");
        /*long s = System.currentTimeMillis();
        Float f = 1234959934953495349534954395F;
        for (int i = 0; i < 100000; i++) {
            BigDecimal b = new BigDecimal(f);
            b = b.add(b);
            System.out.println(b.toString());
        }
        System.out.println(System.currentTimeMillis() - s);*/
        /*Object o = new Object();
        synchronized (o) {
            try {
                o.wait(3 * 1000L);
            } catch (InterruptedException e) {
            }
        }
        System.out.println("over");*/
        /*String time = "00718B5BA724110A58ED95F1000E70F8".substring(16, 24);
        long t = Long.parseLong(time, 16);
        System.out.println(t);*/
       /* Long l = 1491965425L;
        System.out.println(Long.toHexString(l));*/
     }
}
