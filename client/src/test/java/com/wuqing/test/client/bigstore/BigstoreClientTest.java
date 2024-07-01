package com.wuqing.test.client.bigstore;

import com.wuqing.client.bigstore.BigstoreClient;
import com.wuqing.client.bigstore.bean.ColumnDef;
import com.wuqing.client.bigstore.bean.Condition;
import com.wuqing.client.bigstore.bean.DataResult;
import com.wuqing.client.bigstore.bean.ResponseData;
import com.wuqing.client.bigstore.config.Constants;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wuqing on 17/4/11.
 */
public class BigstoreClientTest {

    private static final String targetIp = "127.0.0.1";
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
        //queryLocal();
        //create();
        //load();
        //store();
        //asyncStore();
        query();
        //queryTest();
        //addColumn();
        //flushTable();
        //descTable();
        //queryGrafana();
        //queryTestEnv();
        //addColumn();
        //queryBigData();
        //testQueryElb();
        //testTcp();
        System.exit(0);
    }

    private static void testTcp() {
        BigstoreClient client = new BigstoreClient(targetIp, Constants.PORT, Constants.DEFAULT_DATA_BASE, 3000);
        client.showTables();
    }

    private static void queryLocal() {
        try {
            BigstoreClient client = new BigstoreClient(targetIp, Constants.PORT, Constants.DEFAULT_DATA_BASE);
            Condition con = new Condition("default_data_base");
            //String sql = "select min( distinct(time ))[10] as timefunc from test_table where shuzi between 30 and 33 and time = 16551962 limit 10";
            String sql = "select sum(distinct(time ))[10] as timefunc from test_table where shuzi between 30 and 33 and time in (16551962, 1655196233126) limit 10";
            con.setSql(sql);
            ResponseData response = client.query(con);
            DataResult result = response.getData();
            String column = "";
            String sp = "";
            for (String col : result.getColumns()) {
                column += sp + col;
                sp = ", ";
            }
            System.out.println(column);
            for (List<String> ls : result.getDatas()) {
                System.out.println(ls.toString());
            }
            System.out.println(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    private static void queryBigData() {
        BigstoreClient client = new BigstoreClient("10.70.21.87", Constants.PORT, "bigdata");
        String sql = "select * from fin-process where time between 1639387801000 and 1639474201000 and app_name='bigdata_ods-hufenggang-bigdata_ods.ods_trade_center_test_w_order_goods_dh_f' and log_id=270872";
        Condition condition = new Condition();
        condition.setSql(sql);
        ResponseData responseData = client.query(condition);
        System.out.println(responseData.getData());
        System.exit(0);
    }

    private static void queryTest195() {
        BigstoreClient client = new BigstoreClient("10.100.21.195", Constants.PORT, "devops-business-monitor-alarm-prod");
        for (int i = 0; i < 10000; i++ ) {
            Condition con = new Condition();
            con.setSql("SELECT timestamp FROM dealer-finance_unpolymerize_alarm WHERE timestamp between 1633660970293 and 1633662770293 and businessRuleId = 64 ");
            ResponseData responseData = client.query(con);
            long total = responseData.getData().getTotal();
            System.out.println(i + ":" + total);
            if (total == 0) {
                System.out.println("error");
            }
        }
        //System.out.println(total);
        System.exit(0);
    }

    private static void queryTestEnv() {
        BigstoreClient client = new BigstoreClient("10.70.20.201", Constants.PORT, "dealer-activity");
        Condition con = new Condition();
        //con.setSql("select scount(timestamp)[36000] from sun_access_data_log where timestamp between 1621135380000 and  1621135735000 and appId='market-adcontent' and url != '/actuator/health' group by appId,group,version,url");
        //con.setSql("SELECT * FROM sun_event_data_log WHERE serviceId = 'quick-server' ");
        con.setSql("select * from activity_record limit 10");
        ResponseData responseData = client.query(con);
        long total = responseData.getData().getTotal();
        System.out.println(total);
        System.exit(0);
    }

    private static void descTable() {
        BigstoreClient client = new BigstoreClient("10.100.21.83", Constants.PORT, "businesslog");
        ResponseData responseData = client.descTable("abmau-edi");
        System.out.println(responseData);
    }

    private static void queryGrafana() {
        BigstoreClient client = new BigstoreClient("10.100.21.83", Constants.PORT, "businesslog");
        Condition con = new Condition();
        String sql = "select * from message-dingding where logTime between 1647316920222 and 1647317075222 and logLevel = 'ERROR' limit 0, 10";
        con.setSql(sql);
        /*con.setTable("message-dingding");
        con.setStart(0);
        con.setLimit(10);
        con.addConditionSubList(new ConditionSub().setColumn("logTime").setQueryRanges(Arrays.asList(new QueryRange(1647316920222L, 1647317075222L))));
        con.addConditionSubList(new ConditionSub().setColumn("logLevel").setIn(Arrays.asList("ERROR")));*/

        //con.setSql("select count(time_iso8601) from topic-elb-pro where time_iso8601 between 1608535680000 and 16085428800000");
        //con.setSql("select scount(time_iso8601) from topic-elb-pro where time_iso8601 between 1608481680000 and 16085428800000 group by status");
        //con.setSql("select count(time_iso8601) from topic-elb-pro_his where time_iso8601 between 1605059700000 and 1605060300000 and host='sales-order-api.idanchuang.vpc'  and url like '*/api/graph*' ");
        //con.setSql("select count(time_iso8601) from topic-elb-pro_his where time_iso8601 between 1605059700000 and 1605060300000 ");
        //con.setSql("select scount(logTime) from abmau-edi where logTime between 1616123407000 and 1616124127000 group by logLevel ");
        //con.setSql("select scount(timestamp)[36000] from sun_access_data_log where timestamp between 1621135380000 and  1621135735000 and appId='market-adcontent' and url != '/actuator/health' group by appId,group,version,url");
        //con.setSql("select ssum(point) from member-point-service-point-increase where logTime between 1639641623000 and 1639643680900 group by channelTypeName ");
        ResponseData responseData = client.query(con);
        long total = responseData.getData().getTotal();
        System.out.println(total);
        System.exit(0);
    }

    private static void flushTable() {
        BigstoreClient client = new BigstoreClient("10.100.20.117", Constants.PORT, "elb");
        ResponseData responseData = client.flushTableCache();
        /*client = new BigstoreClient("10.100.20.70", Constants.PORT, "elb");
        responseData = client.flushTableCache();*/
        System.exit(0);
    }

    private static void addColumn() {
        List<String> filter = filterTable();
        String[] ips = new String[] {"10.100.21.83"};
        for (String ip : ips) {
            BigstoreClient client = new BigstoreClient(ip, Constants.PORT, "businesslog");
            ResponseData res = client.showTables();
            DataResult result = res.getData();
            if (result.next()) {
                List<String> list = result.getRow();
                for (String s : list) {
                    if (filter.contains(s)) {
                        continue;   //不变更
                    }
                    if (s.startsWith("sun_")) {
                        continue;
                    }
                    List<ColumnDef> colList = new ArrayList<>();
                    ColumnDef def = new ColumnDef("traceIdTime", ColumnDef.LONG_ORDER_TYPE);
                    colList.add(def);
                    ResponseData responseData = client.addTableColumn(s, colList);
                    System.out.println(s);

                }
            }
        }
        System.out.println("===");

    }

    private static List<String> filterTable() {
        List<String> list = new ArrayList<>();
        list.add("pay-risk-manage-pay-metrics");
        list.add("elb-log");
        list.add("platform-notification-total");
        list.add("dead_letter_publish");
        list.add("dealer_pay_callback_fail");
        list.add("access_route_log");
        list.add("open_data_incr_msg_consume_avg_cost");
        list.add("pay-risk-manage-pay-metrics-1");
        list.add("sentinel-dashboard-metric");
        list.add("log_consume_count");
        list.add("sun_access_data_log");
        list.add("pay-risk-manage-pay-metrics-channel-1");
        list.add("gather-metric");
        list.add("pay-risk-manage-pay-metrics-sql-2");
        list.add("dealer_finance_success");
        list.add("sun_access_data_log_dev");
        list.add("sun_thread_pool_info");
        list.add("service_invoke_count");
        list.add("checkout_source");
        list.add("sms_rpc_from_count");
        list.add("log_consume_time");
        list.add("service_invoke_cost_time");
        list.add("sun_event_data_log");
        list.add("green_mq_send_success");
        list.add("log_produce_count");
        list.add("risk_event_invoke_log");
        list.add("file_upload_cost_time");
        list.add("sms_send_fail");
        list.add("es_invoke_countv");
        list.add("sunClientNumMetrics");
        list.add("open_data_incr_msg_receive_total");
        list.add("abmio_file_incr_sync");
        list.add("sun_event_data_log_dev");
        list.add("stock_not_enough");
        list.add("appSqlRt");
        list.add("green_third_api_invoke_cost");
        list.add("dealer_finance_fail");
        list.add("risk_manage_event");
        list.add("dealer_pay_callback_success");
        list.add("ocr_cost_time");
        list.add("pay-risk-manage-pay-metrics-channel-2");
        list.add("elb-pro");
        list.add("risk_manage_rule_hit");
        list.add("sun_access_data_log_pre");
        list.add("file_upload");
        list.add("open_data_incr_msg_consume_error_count");
        list.add("pay-risk-manage-pay-metrics-sql");
        list.add("pay-channel_api_res_monitor_log_need_delete_9999");
        list.add("sms_send_total");
        list.add("pay-risk-manage-pay-metrics-channel");
        list.add("chat-backup-total");
        list.add("checkout_params");
        list.add("order_relation_fail");
        list.add("twice_checkout");
        list.add("order_relation_process_cost_time");
        list.add("order_relation_wait_cost_time");
        list.add("order_relation_success");
        list.add("member-point-service-point-decrease");
        list.add("member-point-service-finish-task");
        list.add("member-point-service-point-increase");
        list.add("scene_message_log");
        list.add("order_check_failed");
        list.add("gather_tdengine_insert_cost_time");
        list.add("checkout_order_size");
        list.add("platform-notification-failed");
        list.add("alpha_x_global_tx_end_cost");
        list.add("alpha_x_event_consume_time");
        list.add("count_async_event_consume_time");
        list.add("alpha_x_event_task");
        list.add("platform-notification-total");
        list.add("green_third_api_invoke_fail");
        list.add("count_async_event_sent_fail");
        list.add("international_sms_verify_backfill");
        list.add("international_sms_verify_delay");
        list.add("sun_event_data_log_pre");
        list.add("count_async_event_consumed");
        list.add("green_thread_pool_info");
        list.add("es_invoke_count");

        return list;
    }

    private static void queryTest() {
        BigstoreClient client = new BigstoreClient("10.70.20.201", Constants.PORT, "businesslog");
        Condition con = new Condition();
        //con.setSql("select scount(timestamp) from sun_access_data_log where timestamp between 1642664456000 and 1642666256000 and appId = 'abmio' and url = '/batchQuery' and requestMethod = 'POST' group by appId,url,requestMethod");
        con.setSql("select scount(clientIp) from sun_access_data_log where timestamp between 1642664456000 and 1642666256000 and appId = 'abmio' and url = '/batchQuery' and requestMethod = 'POST' group by appId,url,requestMethod");
        ResponseData responseData = client.query(con);
        long total = responseData.getData().getTotal();
        System.out.println(total);
        System.exit(0);

    }

    public static void load() {
        BigstoreClient client = new BigstoreClient(targetIp, Constants.PORT, Constants.DEFAULT_DATA_BASE);
        client.loadData("test_table1", "/tmp/test_data.txt");
        client.stopClient();
    }

    public static void store() {
        int count = 10;

        BigstoreClient client = new BigstoreClient(targetIp, Constants.PORT, Constants.DEFAULT_DATA_BASE);

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    List<String[]> lines = new ArrayList<>();
                    for (int i = 0; i < 10000; i++) {
                        long time = System.currentTimeMillis();;
                        String[] array = (time + "|name|这是一个描述1|extend|1||-1").split("\\|");
                        //String[] array = (time + "|name|这是一个描述1|extend|sss||-1").split("\\|");
                        lines.add(array);
                        array = (time + "|name|这是一个描述2||||1").split("\\|");
                        lines.add(array);
                        array = (time + "|name|这是一个描述3\n|extend|3||-2").split("\\|");
                        lines.add(array);
                        array = (time + "|name|这是一个描述4|||menu4|2").split("\\|");
                        lines.add(array);
                        array = (time + "|name|这是一个描述5|||menu5|4").split("\\|");
                        lines.add(array);
                    }
                    for (int i = 0; i < 100; i++) {
                        ResponseData res = client.storeData("test_table1", lines);
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

    public static void asyncStore() {
        int count = 100;
        BigstoreClient client = new BigstoreClient(targetIp, Constants.PORT, Constants.DEFAULT_DATA_BASE);

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(count);
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    List<String[]> lines = new ArrayList<>();
                    for (int i = 0; i < 2000; i++) {
                        long time = System.currentTimeMillis();;
                        String[] array = (time + "|name|这是一个描述1|extend|1||-1").split("\\|");
                        client.asyncStoreData("test_table1", array);
                        array = (time + "|name|这是一个描述2||||1").split("\\|");
                        client.asyncStoreData("test_table1", array);
                        array = (time + "|name|这是一个描述3|extend|3||-2").split("\\|");
                        client.asyncStoreData("test_table1", array);
                        array = (time + "|name|这是一个描述4|||menu4|2").split("\\|");
                        client.asyncStoreData("test_table1", array);
                        array = (time + "|name|这是一个描述5|||menu5|4").split("\\|");
                        client.asyncStoreData("test_table1", array);
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
        System.out.println("time:" + (System.currentTimeMillis() - start));
        client.stopClient();
        System.exit(0);
    }

    public static void create() {
        BigstoreClient client = new BigstoreClient(targetIp, Constants.PORT, Constants.DEFAULT_DATA_BASE);
        List<ColumnDef> list = new ArrayList<ColumnDef>();
        list.add(new ColumnDef("time", ColumnDef.LONG_ORDER_TYPE, 15));
        list.add(new ColumnDef("name", ColumnDef.STRING_FIND, 10));
        list.add(new ColumnDef("desc", ColumnDef.STRING_TYPE));
        list.add(new ColumnDef("extend", ColumnDef.STRING_TYPE));
        list.add(new ColumnDef("extendLong", ColumnDef.LONG_FIND, 10));
        list.add(new ColumnDef("menu", ColumnDef.STRING_MENU, 5));
        list.add(new ColumnDef("shuzi", ColumnDef.LONG_TYPE));
        ResponseData resp = client.createTable("test_table1", list);
        client.stopClient();
    }

    private static void query() throws InterruptedException {
        BigstoreClient client = new BigstoreClient("127.0.0.1", Constants.PORT, Constants.DEFAULT_DATA_BASE);
        Random r = new Random();
        long time = 153051166510L;
        long end = 15305116651400L;
        for (int ii = 0; ii < 1; ii++) {
            client.restart();
            long s = System.currentTimeMillis();
            Condition con = new Condition();
            /*con.addField("count(time)");
            con.setTable("test_table");
            con.setLimit(10);
            con.setStart(1000);
            //con.addConditionSubList(new ConditionSub().setColumn("startTime").addQueryRange(new QueryRange(time, time + 10 * 1000)));
            con.addConditionSubList(new ConditionSub().setColumn("time").addQueryRange(new QueryRange(time, end)));
            //con.addConditionSubList(new ConditionSub().setColumn("shuzi").setSearch(r.nextInt(5)));*/
            //String sql = "select scount(time)[10] from test_table group by menu limit 10";
            String sql = "select * from test_table1 where desc = '这是一个描述3\n' limit 100";
            con.setSql(sql);
            con.setOneMachineAggregationLimit(1000000000);
            ResponseData res = client.query(con);
            //ResponseData res = client.query(con, false);
            if (res.isSuccess() && res.getData() != null) {
                DataResult result = (DataResult) res.getData();
                //int count = 0;
                while (result.next()) {
                    //count += result.getInt(1);
                    result.getRow();
                    System.out.println(result.getString(2));
                    String sss = result.getString(3);
                    System.out.println(sss);
                }
                //System.out.println(result.getTotal());
                //System.out.println(count);
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
