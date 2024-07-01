package com.wuqing.client.bigstore.bean;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wuqing on 16/11/17.
 * 自定义的封装包
 */
public class MyPackage implements Serializable {

    private static final long serialVersionUID = 1L;

    private static long MAX = 100000000000L;

    private static long UU_ID = MAX * (Math.abs(UUID.randomUUID().hashCode()) % 100000);

    private static AtomicLong INCREASE_FLAG = new AtomicLong(0);

    /**
     * 纳秒，作为包唯一标识
     */
    private long time;

    /**
     * 数据
     */
    private Object data;

    public MyPackage() {
        long increment = INCREASE_FLAG.incrementAndGet();
        time = UU_ID + increment;
        if (increment >= MAX) {
            //上锁避免并发问题
            synchronized (INCREASE_FLAG) {
                if (INCREASE_FLAG.get() >= MAX) {
                    INCREASE_FLAG.set(0);
                }
            }
        }
    }

    public MyPackage(long time) {
        this.time = time;
    }

    public long getTime() {
        return time;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public static void main(String[] args) throws InterruptedException {
        /*for (int ii = 0; ii < 10; ii++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 100000; i++) {
                        MyPackage myPackage = new MyPackage();
                        System.out.println(myPackage.getTime());
                    }
                }
            });
            t.start();
        }*/

    }

}
