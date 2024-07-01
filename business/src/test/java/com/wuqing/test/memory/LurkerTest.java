package com.wuqing.test.memory;

import com.wuqing.client.bigstore.BigstoreClient;
import com.wuqing.client.bigstore.bean.*;
import com.wuqing.client.bigstore.config.Constants;

/**
 * Created by wuqing on 17/5/17.
 */
public class LurkerTest {

    private static BigstoreClient client = new BigstoreClient("10.50.140.22", Constants.PORT, "flink_log");

    public static void main(String[] args) {

        Condition condition = new Condition();
        condition.addConditionSubList(new ConditionSub().setColumn("time").addQueryRange(new QueryRange(1566893847285L, 1566902840678L)));
        condition.setTable("application_1565004825794_0436_container_e28_1565004825794_0436_01_000012");
        ResponseData responseData = client.query(condition);
        DataResult res = responseData.getData();
        System.out.println("total:" + res.getTotal());
        while (res.next()) {
            //System.out.println(res.getRow());
        }
        System.exit(0);
    }

}
