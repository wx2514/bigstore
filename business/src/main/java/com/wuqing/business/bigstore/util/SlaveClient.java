package com.wuqing.business.bigstore.util;

import com.wuqing.business.bigstore.config.Params;
import com.wuqing.client.bigstore.BigstoreClient;
import com.wuqing.client.bigstore.bean.ResponseData;
import com.wuqing.client.bigstore.config.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wuqing on 17/9/20.
 */
public class SlaveClient {

    private final static Logger logger = LoggerFactory.getLogger("send-data-log");

    private static List<BigstoreClient> clientList;

    private static final int SIZE = 3;    //防止有个连接断开，保证可用性

    private static final Random ran = new Random();

    private static boolean existClient = false;

    private static final long SYNC_TIME_OUT = 10000;    //超时默认10秒

    static {
        try {
            clientList = new ArrayList<BigstoreClient>();
            if (Params.getSlaveIp() != null) {
                //开启多个通道
                for (int i = 0; i < SIZE; i++) {
                    clientList.add(new BigstoreClient(Params.getSlaveIp(), Params.getSlavePort(), "", SYNC_TIME_OUT));
                }
                existClient = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private SlaveClient() {

    }

    private static BigstoreClient getSlaveClient() {
        if (existClient) {
            //随机取一个
            return clientList.get(ran.nextInt(SIZE));
        }
        return null;
    }

    /**
     * 主从同步数据
     * @param file
     * @param data
     * @return
     */
    public static boolean syncFile(String file, byte[] data) {
        if (!existClient) {
            return true;    //如果不需要主从同步，就直接返回成功
        }
        if (data == null) {
            data = GZipUtil.readTxt2Byte(file);
        }
        return executeSync(file, data);
    }

    /**
     * 主从同步数据
     * @param file
     * @return
     */
    public static boolean syncFile(String file) {
        if (!existClient) {
            return true;    //如果不需要主从同步，就直接返回成功
        }
        byte[] data = GZipUtil.readTxt2Byte(file);
        return executeSync(file, data);

    }

    /**
     * 同步数据文件，重试 SIZE 次
     * @param file
     * @param data
     * @return
     */
    private static boolean executeSync(String file, byte[] data) {
        boolean res = false;
        for (int i = 0; i < SIZE; i++) {
            try {
                ResponseData response = SlaveClient.getSlaveClient().syncData(file.substring(Params.getBaseDir().length()), data);
                if (response == null  || !response.isSuccess()) {
                    logger.error("sync file to slave fail, file" + file);
                    res = false;
                } else {
                    res = true;
                    break;  //成功了就跳出
                }
            } catch (Exception e) {
                logger.error("sync file to slave fail, file" + file, e);
            }
        }
        return res;
    }

    public static boolean haveSlave() {
        return existClient;
    }

}
