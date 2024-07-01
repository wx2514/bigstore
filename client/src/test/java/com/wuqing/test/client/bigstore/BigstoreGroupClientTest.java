package com.wuqing.test.client.bigstore;

import com.wuqing.client.bigstore.BigstoreGroupClient;
import com.wuqing.client.bigstore.bean.ColumnDef;
import com.wuqing.client.bigstore.bean.Condition;
import com.wuqing.client.bigstore.bean.HostConfig;
import com.wuqing.client.bigstore.bean.ResponseData;

import java.util.ArrayList;
import java.util.List;

public class BigstoreGroupClientTest {

    public static void main(String[] args) {
        test6();

    }

    private static void test6() {
        BigstoreGroupClient groupClient = new BigstoreGroupClient("skywalking",
                new HostConfig("10.100.20.72"), new HostConfig("10.100.20.173"));

        //BigstoreClient groupClient = new BigstoreClient("10.100.20.173", 60000,"skywalking");

        for (int i = 0; i < 100; i++) {

            //String sql = "select segment_id, start_time, endpoint_name, latency,is_error,trace_id from segment where trace_id = 'f1d09f3cefe342aeb20003bb69e50b9d.1905.16391207894155193' and trace_id_timestamp=16391207894155193 limit 100";
            //String sql = "select segment_id, start_time, endpoint_name, latency,is_error,trace_id from segment where trace_id_timestamp between 16391207894155193 and 16391207894155193 and trace_id = 'f1d09f3cefe342aeb20003bb69e50b9d.1905.16391207894155193'  limit 100";

            //String sql = "select segment_id, start_time, endpoint_name, latency,is_error,trace_id from segment where trace_id_timestamp=16393849003665687 and trace_id ='d3654088cc1545b79cd7fbc9960a9ad0.126.16393849003665687' limit 100";
            String sql = "select segment_id, start_time, endpoint_name, latency,is_error,trace_id from segment where trace_id ='d3654088cc1545b79cd7fbc9960a9ad0.126.16393849003665687' and trace_id_timestamp=16393849003665687 limit 100";

            Condition con = new Condition();
            con.setSql(sql);
            try {
                ResponseData response = groupClient.query(con);
                System.out.println(response.isSuccess() + ":" + response.getData());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("over");
        System.exit(0);

    }

    private static void test5() {
        BigstoreGroupClient groupClient = new BigstoreGroupClient("default_data_base",
                new HostConfig("127.0.0.1"), new HostConfig("127.0.0.1"), new HostConfig("127.0.0.1"));
        for (int i = 0; i < 100; i++) {

            Condition con = new Condition();
            con.setSql("select * from test_table limit 10");
            ResponseData res = groupClient.query(con);
            System.out.println(res.isSuccess());

        }
    }

    private static void test1() {
        BigstoreGroupClient groupClient = new BigstoreGroupClient("default_data_base",
                new HostConfig("127.0.0.1"), new HostConfig("127.0.0.1"), new HostConfig("127.0.0.1"));
        ResponseData res = groupClient.showDatabases();
        System.out.println(res);
        res = groupClient.descTable("test_table");
        System.out.println(res);
        res = groupClient.asyncStoreData("test_table", getLine());
        System.out.println(res);
        List<ColumnDef> colList = new ArrayList<>();
        colList.add(new ColumnDef("time", ColumnDef.LONG_ORDER_TYPE));
        res = groupClient.createTable("test_ttt", colList);
        System.out.println(res);
    }

    private static void test2() {
        BigstoreGroupClient groupClient = new BigstoreGroupClient("default_data_base",
                new HostConfig("127.0.0.1"), new HostConfig("127.0.0.1"), new HostConfig("127.0.0.1"));
        ResponseData res = groupClient.asyncStoreData("test_table", getLine());
        res = groupClient.asyncStoreData("test_table", getLine());
        res = groupClient.asyncStoreData("test_table", getLine());
        res = groupClient.asyncStoreData("test_table", getLine());
        res = groupClient.asyncStoreData("test_table", getLine());
        res = groupClient.asyncStoreData("test_table", getLine());
        res = groupClient.asyncStoreData("test_table", getLine());
        res = groupClient.asyncStoreData("test_table", getLine());
        res = groupClient.asyncStoreData("test_table", getLine());
        res = groupClient.asyncStoreData("test_table", getLine());
        res = groupClient.asyncStoreData("test_table", getLine());

        System.out.println(res);

        System.exit(0);

    }

    private static void test3() {
        BigstoreGroupClient groupClient = new BigstoreGroupClient("default_data_base",
                new HostConfig("127.0.0.1"), new HostConfig("127.0.0.1"), new HostConfig("127.0.0.1"));
        ResponseData res = groupClient.asyncStoreData("test_table", getLine());
        res = groupClient.asyncBatchStoreData("test_table", getBatchLine());
        res = groupClient.asyncBatchStoreData("test_table", getBatchLine());
        res = groupClient.asyncBatchStoreData("test_table", getBatchLine());
        res = groupClient.asyncBatchStoreData("test_table", getBatchLine());
        res = groupClient.asyncBatchStoreData("test_table", getBatchLine());
        res = groupClient.asyncBatchStoreData("test_table", getBatchLine());
        res = groupClient.asyncBatchStoreData("test_table", getBatchLine());
        res = groupClient.asyncBatchStoreData("test_table", getBatchLine());
        res = groupClient.asyncBatchStoreData("test_table", getBatchLine());
        res = groupClient.asyncBatchStoreData("test_table", getBatchLine());

        System.out.println(res);

        System.exit(0);

    }


    private static String[] getLine() {
        String[] s = new String[8];
        for (int i = 0; i < 8; i++) {
            s[i] = String.valueOf(System.currentTimeMillis());
        }
        return s;
    }

    private static List<String[]> getBatchLine() {
        List<String[]> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(getLine());
        }
        return list;
    }
}
