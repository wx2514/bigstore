package com.wuqing.test.client.bigstore;

import com.wuqing.client.bigstore.bean.DataPack;
import com.wuqing.client.bigstore.util.HessianUtil;
import com.wuqing.client.bigstore.util.KryoUtil;
import com.wuqing.client.bigstore.util.SerializeUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by wuqing on 17/7/6.
 */
public class SeqCompare {

    private final static int TEST_SIZE = 100;

    public static void main(String[] args) {
        List<Object> list = new ArrayList<Object>();
        Random ran = new Random();
        for (int i = 0; i < 30000; i++) {
            //list.add("aaa|sss|ddd|fff|ggg|hhh|jjj|kkk|lll");
            DataPack dp = new DataPack(String.valueOf(ran.nextInt(100) ), ran.nextInt(100));
            list.add(dp);
            //list.add(ran.nextInt(100) + "|" + ran.nextInt(100) + "|" + ran.nextInt(100) + "|" + ran.nextInt(100) + "|" + ran.nextInt(100) + "|" + ran.nextInt(100));
        }
        System.out.println();
        long s = System.currentTimeMillis();
        byte[] b = null;
        for (int i = 0; i < TEST_SIZE; i++) {
            b = SerializeUtil.serialize(list);
            SerializeUtil.unserialize(b);
        }
        System.out.println("java:" + (System.currentTimeMillis() - s) + ", length:" + b.length);

        s = System.currentTimeMillis();
        for (int i = 0; i < TEST_SIZE; i++) {
            b = HessianUtil.serialize(list);
            HessianUtil.unserialize(b);
        }
        System.out.println("hessian:" + (System.currentTimeMillis() - s) + ", length:" + b.length);

        s = System.currentTimeMillis();
        for (int i = 0; i < TEST_SIZE; i++) {
            b = KryoUtil.writeToByteArray(list);
            Object o = KryoUtil.readFromByteArray(b);
            int k = 0;
        }
        System.out.println("kryo:" + (System.currentTimeMillis() - s) + ", length:" + b.length);

    }

}
