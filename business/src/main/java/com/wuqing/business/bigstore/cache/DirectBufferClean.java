package com.wuqing.business.bigstore.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

public class DirectBufferClean {

    private final static Logger logger = LoggerFactory.getLogger("cache-log");

    /**
     * 保存清理方法（反射获取）
     * 触发此方法，手动释放堆外内存
     */
    private static Method cleanerMethod = null;

    static {
        try {
            Class cls = Class.forName("java.nio.DirectByteBuffer");
            Method m = cls.getMethod("cleaner");
            m.setAccessible(true);
            cleanerMethod = m;
        } catch (Exception e) {
            logger.error("invoke static get clean method fail.", e);
        }
    }

    public static void clean(ByteBuffer value) {
        try {
            if (cleanerMethod != null) {    //释放直接内存
                //long s = System.currentTimeMillis();
                Cleaner c = (Cleaner) cleanerMethod.invoke(value);
                c.clean();
                c.clear();
                //queryTimeLogger.debug("auto remove direct memory, time:" + (System.currentTimeMillis() - s));
            }
        } catch (Exception e) {
            logger.error("when buffer release, clean direct memory", e);
        }
    }
}
