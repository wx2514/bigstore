package com.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Date;

/**
 * Created by wuqing on 17/7/10.
 */
public class FileTest {

    public static void main(String[] args){
        FileChannel channel = null;
        FileLock lock = null;
        try {
            //1. 对于一个只读文件通过任意方式加锁时会报NonWritableChannelException异常
            //2. 无参lock()默认为独占锁，不会报NonReadableChannelException异常，因为独占就是为了写
            //3. 有参lock()为共享锁，所谓的共享也只能读共享，写是独占的，共享锁控制的代码只能是读操作，当有写冲突时会报NonWritableChannelException异常
            File f = new File("/Users/wuqing/log.txt");
            channel = new FileOutputStream(f,true).getChannel();

            //获得锁方法一：lock()，阻塞的方法，当文件锁不可用时，当前进程会被挂起
            //lock = channel.lock();//无参lock()为独占锁
            //lock = channel.lock(0L, Long.MAX_VALUE, true);//有参lock()为共享锁，有写操作会报异常

            //获得锁方法二：trylock()，非阻塞的方法，当文件锁不可用时，tryLock()会得到null值
            //do {
            //  lock = channel.tryLock();
            //} while (null == lock);
            lock = channel.tryLock();
            if (lock == null) {
                return;
            }

            //互斥操作
            ByteBuffer sendBuffer = ByteBuffer.wrap((new Date() + " 写入\n").getBytes());
            channel.write(sendBuffer);
            f.delete();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (lock != null) {
                try {
                    lock.release();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (channel != null) {
                try {
                    channel.close();
                    channel = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
