package com.wuqing.business.bigstore.util;

import com.wuqing.business.bigstore.config.ServerConstants;
import com.wuqing.client.bigstore.util.BloomFilter;
import com.wuqing.client.bigstore.util.KryoUtil;
import com.wuqing.client.bigstore.util.MisjudgmentRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.wuqing.client.bigstore.config.Constants.SPACE_SIZ;

/**
 * 创建对象的工具类
 */
public class BloomUtil {

    private final static Logger logger = LoggerFactory.getLogger(BlockLockUtil.class);

    /**
     * 创建表分区的布隆过滤器
     * @return
     */
    public static BloomFilter createSpaceBloom() {
        return new BloomFilter((int) (ServerConstants.PARK_SIZ * SPACE_SIZ), MisjudgmentRate.HIGH);
    }

    /**
     * 创建数据快的布隆过滤器
     * @return
     */
    public static BloomFilter createBlockBloom() {
        return new BloomFilter((int) (ServerConstants.PARK_SIZ), MisjudgmentRate.SMALL);
    }

    public static BloomFilter readBloom(String file) {
        return readBloom(new File(file));
    }

    public static BloomFilter readBloom(File file) {
        try {
            byte[] bytes = FileUtil.read2Byte(file);
            return  (BloomFilter) KryoUtil.readFromByteArray(bytes);
        } catch (Exception e) {
            logger.error("read bloom fail", e);
            return null;
        }
    }

    public static void writeBloom(BloomFilter bloomFilter, String file) {
        try {
            byte[] bytes = KryoUtil.writeToByteArray(bloomFilter);
            FileUtil.writeByte(file, bytes);
        } catch (Exception e) {
            logger.error("write bloom fail", e);
        }
    }

    public static void main(String[] args) {
        BloomFilter filter = readBloom("/Users/wangxu/bigstore/default_data_base/test_table/rows/0000000010/0000001090/extend_bloom.bt");
        long s = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            filter.exist("extend55575");
        }
        System.out.println("time: " + (System.currentTimeMillis() - s));
    }

}
