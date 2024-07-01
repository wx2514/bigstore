package com.wuqing.test.memory;

import java.nio.ByteBuffer;

/**
 * Created by wuqing on 17/3/3.
 */
public class ByteBufferTest {

    public static String text = "sdfafsa第三方发方法fdsfdsfdffdfssdfdsfdsfdsfdfal;k;skf;sdkf;dlsfkds;fkd;flklfklklklklklklklklkllklklklksd;flksdfk";

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        int length = text.length();
        for (int i = 0; i < 100; i++) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(text.getBytes().length);
            buffer.put(text.getBytes());
        }
        System.out.println(System.currentTimeMillis() - start);
        /*ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
        buffer.put("666".getBytes());
        buffer.flip();
        System.out.println(buffer.position());
        System.out.println(buffer.limit());
        byte[] b = new byte[3];
        buffer.get(b, 0, buffer.limit());
        System.out.println(new String(b));*/
    }

}
