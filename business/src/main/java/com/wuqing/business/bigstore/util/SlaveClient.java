package com.wuqing.business.bigstore.util;

import com.wuqing.business.bigstore.config.Params;
import com.wuqing.client.bigstore.BigstoreClient;
import com.wuqing.client.bigstore.config.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by wuqing on 17/9/20.
 */
public class SlaveClient {

    //private static SlaveClient myslef = new SlaveClient();

    private static List<BigstoreClient> clientList;

    private static Random random = new Random();

    private static int size = 1;

    private static boolean existClient = false;

    static {
        try {
            clientList = new ArrayList<BigstoreClient>();
            if (Params.getSlaveIp() != null) {
                //开启多个通道
                for (int i = 0; i < size; i++) {
                    clientList.add(new BigstoreClient(Params.getSlaveIp(), Params.getSlavePort(), "", 5000));
                }
                existClient = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private SlaveClient() {

    }

    public static BigstoreClient getSlaveClient() {
        if (existClient) {
            return clientList.get(random.nextInt(size));
        }
        return null;
    }

}
