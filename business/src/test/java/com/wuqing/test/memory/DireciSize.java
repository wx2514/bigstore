package com.wuqing.test.memory;

import java.lang.reflect.Field;

/**
 * Created by wuqing on 17/7/6.
 */
public class DireciSize {

    public static void main(String[] args) throws Exception {
        Class<?> c = Class.forName("java.nio.Bits");
        Field maxMemory = c.getDeclaredField("maxMemory");
        maxMemory.setAccessible(true);
        Field reservedMemory = c.getDeclaredField("reservedMemory");
        reservedMemory.setAccessible(true);
        Long maxMemoryValue = (Long)maxMemory.get(null);
        Long reservedMemoryValue = (Long)reservedMemory.get(null);
        System.out.println("maxMemoryValue:" + maxMemoryValue);
        System.out.println("reservedMemoryValue:" + reservedMemoryValue);
    }

}
