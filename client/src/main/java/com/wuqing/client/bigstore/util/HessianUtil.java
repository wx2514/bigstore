package com.wuqing.client.bigstore.util;

import com.caucho.hessian.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by wuqing on 17/7/6.
 * hession 序列化 反序列化 工具
 */
public class HessianUtil {

    private static Logger logger = LoggerFactory.getLogger(HessianUtil.class);

    private static final SerializerFactory HESSIAN_SERIALIZER_FACTORY = new SerializerFactory();

    /**
     * 序列化
     * @param object
     * @return
     */
    public static byte[] serialize(final Object object) {
        ByteArrayOutputStream os = null;
        HessianSerializerOutput ho = null;
        try {
            os = new ByteArrayOutputStream();
            //Hessian的序列化输出
            ho = new HessianSerializerOutput(os);
            ho.setSerializerFactory(HESSIAN_SERIALIZER_FACTORY);
            ho.writeObject(object);
            close(ho);
            return os.toByteArray();
        } catch (Exception e) {
            logger.error("serialize fail", e);
        } finally {
            CommonUtil.close(os);
        }
        return null;
    }

    /**
     * 反序列化
     * @param bytes
     * @return
     */
    public static Object unserialize(final byte[] bytes) {
        ByteArrayInputStream is = null;
        HessianSerializerInput hi = null;
        try {
            is = new ByteArrayInputStream(bytes);
            //Hessian的反序列化读取对象
            hi = new HessianSerializerInput(is);
            hi.setSerializerFactory(HESSIAN_SERIALIZER_FACTORY);
            return hi.readObject();
        } catch (Exception e) {
            logger.error("unserialize fail", e);
        } finally {
            CommonUtil.close(is);
            close(hi);
        }
        return null;
    }

    public static void close(Hessian2Output closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
        }
    }

    public static void close(Hessian2Input closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
        }
    }
}
