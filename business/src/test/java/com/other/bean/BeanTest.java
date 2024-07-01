package com.other.bean;

import com.wuqing.business.bigstore.util.CopyUtil;

public class BeanTest {

    public static void main(String[] args) {
        TestBean1 t1 = new TestBean1();
        TestBean2 t2 = new TestBean2();
        long start = System.currentTimeMillis();
        CopyUtil.copyProperty(t1, t2, "press");
        System.out.println(System.currentTimeMillis() - start);
        long s = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            CopyUtil.copyProperty(t1, t2, "press");
        }
        System.out.println(System.currentTimeMillis() - s);
    }

}
