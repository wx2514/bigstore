package com.wuqing.test.memory;

import com.wuqing.business.bigstore.util.FileUtil;
import com.wuqing.client.bigstore.util.HessianUtil;

import java.util.List;

/**
 * Created by wuqing on 18/6/6.
 */
public class SerializeTest {

    public static void main(String[] args) throws Exception {
        List<String> list = FileUtil.readAll("/Users/wuqing/pay_insurance_service_20180605_1528172328602", false);
        long s = System.currentTimeMillis();
        HessianUtil.serialize(list);
        System.out.println("time:" + (System.currentTimeMillis() - s));
    }

}
