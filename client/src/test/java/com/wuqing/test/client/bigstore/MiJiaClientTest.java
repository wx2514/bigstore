package com.wuqing.test.client.bigstore;

import com.wuqing.client.bigstore.BigstoreClient;
import com.wuqing.client.bigstore.bean.ColumnDef;
import com.wuqing.client.bigstore.bean.Condition;
import com.wuqing.client.bigstore.bean.DataResult;
import com.wuqing.client.bigstore.bean.ResponseData;
import com.wuqing.client.bigstore.config.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wuqing on 17/4/11.
 */
public class MiJiaClientTest {

    private static final String targetIp = "172.16.4.30";
    //private static final String targetIp = "127.0.0.1";
    //private static String targetIp = "10.50.140.20";
    //private static final String targetIp = "10.100.21.111";
    //private static String targetIp =  "10.70.99.65";
    //private static String targetIp = "10.50.140.19";
    //private static String targetIp = "172.17.45.60";

    /**
     * @param args
     * @throws InterruptedException
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {
        //create();
        //load();
        //store();
        //query();
        //queryTest();
        //addColumn();
        flushTable();
        //descTable();
        System.exit(0);
    }


    private static void flushTable() {
        BigstoreClient client = new BigstoreClient(targetIp, Constants.PORT, Constants.DEFAULT_DATA_BASE);
        ResponseData responseData = client.flushTableCache();
        /*client = new BigstoreClient("10.100.20.70", Constants.PORT, "elb");
        responseData = client.flushTableCache();*/
        System.exit(0);
    }


    public static void load() {
        BigstoreClient client = new BigstoreClient(targetIp, Constants.PORT, Constants.DEFAULT_DATA_BASE);
        client.loadData("test_table1", "/tmp/test_data.txt");
        client.stopClient();
    }

    public static void store() {
        int count = 3000;
        final AtomicInteger idx = new AtomicInteger();
        BigstoreClient client = new BigstoreClient(targetIp, Constants.PORT, Constants.DEFAULT_DATA_BASE);

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    List<String[]> lines = new ArrayList<>();
                    //Random ran = new Random();
                    for (int i = 0; i < 10000; i++) {
                        long time = System.currentTimeMillis();;
                        String[] array = (i + "|" + idx.incrementAndGet() + "|no|" + UUID.randomUUID().toString().substring(0, 12) + "|once|" + time + "|" + time).split("\\|");
                        lines.add(array);
                    }
                    for (int i = 0; i < 10; i++) {
                        ResponseData res = client.storeData("tbl_case_form_zj", lines);
                        System.out.println("store over " + i);
                    }
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
        client.stopClient();
        System.exit(0);
    }

    public static void create() {
        BigstoreClient client = new BigstoreClient(targetIp, Constants.PORT, Constants.DEFAULT_DATA_BASE);
        List<ColumnDef> list = new ArrayList<ColumnDef>();
        list.add(new ColumnDef("bus_id", ColumnDef.LONG_ORDER_TYPE));
        list.add(new ColumnDef("img_id", ColumnDef.LONG_ORDER_TYPE));
        list.add(new ColumnDef("key", ColumnDef.STRING_TYPE));
        list.add(new ColumnDef("value", ColumnDef.STRING_ORDER));
        list.add(new ColumnDef("handle_once", ColumnDef.STRING_TYPE));
        list.add(new ColumnDef("create_time", ColumnDef.LONG_FIND));
        list.add(new ColumnDef("update_time", ColumnDef.LONG_FIND));
        ResponseData resp = client.createTable("tbl_case_form_zj", list);
        client.stopClient();
    }

    private static void query() throws InterruptedException {
        BigstoreClient client = new BigstoreClient(targetIp, Constants.PORT, "mishu");
        Random r = new Random();
        long time = 153051166510L;
        long end = 15305116651400L;
        for (int ii = 0; ii < 1; ii++) {
            client.restart();
            long s = System.currentTimeMillis();
            Condition con = new Condition();
            con.setIgnoreCount(true);
            //String sql = "SELECT bus_id,img_id,key,value,handle_once,create_time,update_time  FROM tbl_case_form_zj where value = 'f-47a1-aad1-c1c84d84b9c6' and key = 'no' limit 10";
            String sql = "SELECT bus_id,img_id,key,value,handle_once,create_time,update_time  FROM tbl_case_form_zj where value = 'ij5kdoxC9po=' and key = 'no' limit 1";
            //String sql = "SELECT bus_id,img_id,key,value,handle_once,create_time,update_time  FROM tbl_case_form_zj where value = 'gLqU80HTzwc=' and key = 'no' limit 1";
            //String sql = "SELECT bus_id,img_id,key,value,handle_once,create_time,update_time  FROM tbl_case_form_zj where value = 'CDapg2fBbI8=' and key = 'no' limit 1";
            //String sql = "SELECT bus_id,img_id,key,value,handle_once,create_time,update_time  FROM tbl_case_form_zj where img_id = '12528882'  limit 1";


            //String sql = "SELECT bus_id,img_id,key,value,handle_once,create_time,update_time  FROM tbl_case_form_zj where value = 'W1yQUAUZ773bTSlODMak/A==' and key = 'no' limit 1";
            //String sql = "SELECT bus_id,img_id,key,value,handle_once,create_time,update_time  FROM tbl_case_form_zj where img_id = '25413055'  limit 1";

            con.setSql(sql);
            con.setOneMachineAggregationLimit(1000000000);
            ResponseData res = client.query(con);
            //ResponseData res = client.query(con, false);
            if (res.isSuccess() && res.getData() != null) {
                DataResult result = (DataResult) res.getData();
                while (result.next()) {
                    System.out.println(result.getRow());
                }

                System.out.println("total:" + result.getTotal());

            }
            time += 1L;
            System.out.println("time:" + (System.currentTimeMillis() - s));
            System.out.println();
        }
        //Thread.sleep(30 * 1000L);
        client.stopClient();
    }

    private static void testQueryElb() {
        BigstoreClient client = new BigstoreClient("10.100.20.117", Constants.PORT, "elb");
        String sql = "select scount(time_iso8601)[300000] from topic-elb-pro where time_iso8601 between 1650361364000 and 1650361374000 and elb_name = 'loadbalancer_d9cf0100-542e-44bf-811f-755765e9a1bd' group by elb_name";
        Condition con = new Condition();
        con.setSql(sql);
        long time = System.currentTimeMillis();
        ResponseData res = client.query(con);
        System.out.println("time:" + (System.currentTimeMillis() - time));
        System.exit(0);
    }

}
