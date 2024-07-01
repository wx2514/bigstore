package com.lock;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by wuqing on 17/10/23.
 */
public class LockTest {

    private static ReentrantLock lock = new ReentrantLock();

    public static void main(String[] args) {
        lock.lock();
        lock.unlock();
        lock.unlock();
        lock.unlock();
        System.out.println("sss");
    }

}
