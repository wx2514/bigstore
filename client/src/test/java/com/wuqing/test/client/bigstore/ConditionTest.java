package com.wuqing.test.client.bigstore;

import com.wuqing.client.bigstore.bean.ColumnCondition;
import com.wuqing.client.bigstore.bean.QueryRange;

public class ConditionTest {

    public static void main(String[] args) {
        ColumnCondition con = new ColumnCondition("database");
        con.setColumn("time").addQueryRange(new QueryRange(1, 100))
                .addQueryRange(new QueryRange(100, 111))
                .addQueryRange(new QueryRange(111, 222))
                .addQueryRange(new QueryRange(222, 333))
                .addQueryRange(new QueryRange(333, 444))
                .addQueryRange(new QueryRange(444, 1555))
                .addQueryRange(new QueryRange(555, 777))
                .addQueryRange(new QueryRange(777, 888))
                .addQueryRange(new QueryRange(999, 12342300));
        for (int i = 0; i < 200000; i++) {
            con.addQueryRange(new QueryRange(12345, 23456));
        }
        String key = con.toCountKey();
        System.out.println(key);
    }

}
