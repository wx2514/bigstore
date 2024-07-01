package com.wuqing.test.memory;

/**
 * Created by wuqing on 17/3/3.
 */
public class FreeMemoryTask implements Runnable {
    private long address = 0;

    public FreeMemoryTask(long address) {
        this.address = address;
    }

    @Override
    public void run() {
        System.out.println("runing FreeMemoryTask");

        if (address == 0) {
            System.out.println("already released");
        } else {
            //Unsafe.getUnsafeInstance().freeMemory(address);
        }

    }
}
