package com.wuqing.business.bigstore.run;

import com.wuqing.business.bigstore.cache.DataCache;
import com.wuqing.business.bigstore.service.BigstoreService;
import com.wuqing.business.bigstore.thread.CleanRunnable;
import com.wuqing.business.bigstore.thread.CompressRunnable;
import com.wuqing.business.bigstore.util.FileUtil;
import com.wuqing.client.bigstore.bean.ColumnDef;
import com.wuqing.client.bigstore.bean.Condition;
import com.wuqing.client.bigstore.bean.DataResult;
import com.wuqing.client.bigstore.config.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Created by wuqing on 17/2/17.
 * 起始函数
 */
public class TestMain {

    private static int type = 1;

    private static int totalByMo = 0;

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        CompressRunnable.start();
        CleanRunnable.start();
        DataCache.directCheck();
        try {
            //long[] dataList = createData();
            //storeColumn("test_table", "time", dataList);
            //System.out.println("over, time:" + (System.currentTimeMillis() - start));
            Random r = new Random();
            if (type == 0) {    //创建表
                List<ColumnDef> list = createTableData();
                BigstoreService.createTable(Constants.DEFAULT_DATA_BASE, "test_table", list);
            } else if (type == 1) { //插入数据
                //List<String> lines = createLogData();
                //List<String[]> lines = createLogDataBySplit();
                //List<String> lines = createLogData2();
                int forCout = 100;
                for (int i = 0; i < forCout; i++) {
                    List<String[]> lines = createLogDataBySplit();
                    BigstoreService.storeTable(Constants.DEFAULT_DATA_BASE, "test_table", lines);
                }
                System.out.println(totalByMo);
            } else if (type == 2) { //本地load数据
                for (int i = 0; i < 10; i++) {
                    List<String> lines = createLogData();
                    String filePath = "/tmp/test_data.txt";
                    FileUtil.writeFile(filePath, lines, false);
                    BigstoreService.loadTable(Constants.DEFAULT_DATA_BASE, "test_table", filePath);
                }
            } else if (type == 3) { //查询数据
                long time = 160396163259L;
                long end = 16039616328850L;
                /*long time = 1505373783283L;
                long end = 1505373965647L;*/
                //long end = 1505110055225L;
                //int houzhui = 31505;
                for (int i = 0; i < 10; i++) {
                    /*time += 10;
                    end += 10;*/
                    Condition con = new Condition(Constants.DEFAULT_DATA_BASE);
                    con.setIgnoreCount(true);
                    //String sql = "select * from   test_table where time between 1527827155537 and  1527827156408 and  menu='menu0' and desc search '1497544081' limit 2,2";
                    //String sql = "select * from   test_table where time between 0 and  99999999999999 limit 100";
                    //String sql = "select * from test_table limit 10";
                    //String sql = "select * from   test_table where time between 1598346533243 and  1598346533573 limit 0, 10";

                    //String sql = " select * from test_table where desc likeor ('*描述1*','*描述2*') limit 100";
                    //String sql = " select * from test_table where id = 'sss' limit 10";
                    //String sql = "select * from operate_log where id = 98 limit 10";
                    //String sql = "select * from operate_log limit 10";
                    //con.setSql(sql);

                    //String sql = "select ssum(time) from test_table group by menu,name limit 10";
                    //String sql = "select scount(time)[10] from test_table where shuzi between 30 and 33 group by menu limit 10";
                    //String sql = "select distinct(time) from test_table where shuzi between 30 and 33 group by menu limit 10";
                    //String sql = "select count( distinct(time ))[10] from test_table where shuzi between 30 and 33 limit 10";
                    //String sql = "select sum( distinct(time ))[10] from test_table where shuzi between 30 and 33 limit 10";
                    //String sql = "select ssum(time) from test_table where shuzi between 30 and 33 limit 10";
                    //String sql = "select  ID as idas, window_start, scount( distinct(time ))[10] as timecount from test_table where shuzi between 30 and 33 group by menu limit 10";
                    //String sql = "select  menu, shuzi from test_table where shuzi between 30 and 33 group by menu, shuzi limit 100";
                    //String sql = "select  time from test_table where shuzi between 30 and 33  limit 10";
                    //String sql = "select time from test_table where time between 152782715553 and 15278271564080 and extendLong = 10 limit 10";

                    //String sql = "select sdelta(time)[1] as dt from test_table where shuzi between 30 and 50 and dt between 120 and 999999999999 group by menu limit 10";
                    //String sql = "select sdelta(time)[1] as dt from test_table where shuzi between 30 and 50 and dt likeand ('1*', '*1') group by menu limit 10";
                    //String sql = "select sdelta(time)[1] as dt from test_table where shuzi between 30 and 50 and dt = 122 group by menu limit 10";
                    //String sql = "select sdelta(time)[1] as dt from test_table where shuzi between 30 and 50 and dt = 122 group by menu limit 10";
                    //String sql = "select sdelta(time)[1] from test_table where shuzi between 30 and 50  group by menu limit 10";
                    //String sql = "select scount(time)[3600] from   test_table where time between 1 and  99999999999999 and shuzi between 10 and 99  group by desc, shuzi";
                    //String sql = "select ssum(shuzi)[3600], scount(shuzi)[3600] from   test_table where time between 1 and  99999999999999 and shuzi between 10 and 99 and id = 80  ";
                    //String sql = "select * from   test_table where time between 1 and  99999999999999 and shuzi between 10 and 99 and id = 80  ";
                    //String sql = "select * from test_table where time between 1 and  99999999999999 and shuzi between 10 and 20 and id between 9000 and 10000  ";
                    //String sql = "select * from test_table where time between 0 and 99999999999999 and id = 10000  ";
                    //String sql = "select * from test_table where time between 0 and 99999999999999 and shuzi in(10,11,12) and menu in('menu0', 'menu1')";
                    //String sql = "select * from test_table where time between 0 and 99999999999999 and menu = 'menu1' limit 0, 10";
                    //String sql = "select * from test_table where time between 0 and 99999999999999 and extend = 'extend72010360' ";
                    //String sql = "select count(dec) as ddd from test_table where time between 0 and 99999999999999  and menu in('menu0', 'menu2') limit 10";
                    //String sql = "select time, shuzi, dec, menu as mm from test_table where time between 0 and 99999999999999  and mm in('menu0', 'menu2') limit 10000";
                    //String sql = "select time, shuzi, dec, menu as mm from test_table where mm in('menu0', 'menu2') and time between 0 and 99999999999999 limit 10000";
                    //String sql = "select scount(time)[2] as ct from test_table where time between 0 and 99999999999999  and ct between 284946 and 99999999999999 and ct in ('475059', '111') limit 10000";
                    //String sql = "select segment_id, start_time, endpoint_name, latency,is_error,trace_id from segment where trace_id = 'f1d09f3cefe342aeb20003bb69e50b9d.1905.16391207894155193' and trace_id_timestamp=16391207894155193 limit 100";
                    //String sql = "select content from cthulhu_task_result where instance_id = 1016406845232063858";
                    //String sql = "select dec from  test_table where time between 0 and  99999999999999 and extendLong = 100";
                    //String sql = "select uuid from test_table where shuzi = '32' and uuid = '809e1cc9-102b-430c-8743-07177cbc6258' limit 10";
                    String sql = "select shuzi from test_table where shuzi = '32' limit 10000, 21000";
                    con.setSql(sql);
                    //con.addConditionSubList((new ConditionSub()).setColumn("desc").setLikeNot("*描述1*"));
                    //con.addConditionSubList((new ConditionSub()).setColumn("menu").setSearchNot("menu0"));
                    //con.addConditionSubList((new ConditionSub()).setColumn("extend").setSearchNot("extend"));
                    //con.setOneMachineAggregationLimit(1000000);
                    /*con.setTable("test_table");
                    con.setLimit(20);
                    con.setStart(0);*/
                    //con.addField("count(time)");
                    //con.addField("scount(time)");
                    //con.addField("ssum(time)");
                    /*if (i < 5) {
                        con.addField("ssum(time)");
                    } else {
                        con.addField("smin(time)");
                    }*/

                    /*con.setGroupBy("menu");

                    con.addConditionSubList(new ConditionSub().setColumn("time").addQueryRange(new QueryRange(time, end)));*/

                    /*List<String> searchList = new ArrayList<>();
                    searchList.add("*描述2*");
                    searchList.add("*描述1*");
                    con.addConditionSubList(new ConditionSub().setColumn("desc").setLikeOr(searchList));*/

                    /*List<String> searchList = new ArrayList<>();
                    searchList.add("*描述3*");
                    searchList.add("*描述1*");
                    con.addConditionSubList(new ConditionSub().setColumn("desc").setLikeOr(searchList));*/

                    //con.addField("id");
                    //con.addField("avg(time)").addField("sum(time)").addField("count(time)").addField("min(time)").addField("max(time)");
                    //con.addField("time").addField("name").addField("desc").addField("extend").addField("extendLong").addField("menu");
                    //con.addField("time").addField("name").addField("desc").addField("extend").addField("extendLong");
                    //con.addConditionSubList(new ConditionSub().setColumn("time").setSearch(time));
                    //变化模式，不会使用到查询缓存
                    //con.addConditionSubList(new ConditionSub().setColumn("time").addQueryRange(new QueryRange(time, end)));
                    //con.addConditionSubList(new ConditionSub().setColumn("desc").setGrep("getBaseUserInfo"));
                    //con.addConditionSubList(new ConditionSub().setColumn("desc").setFulltextRetrieval("sdfsdf+="));
                    //con.addConditionSubList(new ConditionSub().setColumn("desc").setFulltextRetrieval("1497577273130"));
                    //con.addConditionSubList(new ConditionSub().setColumn("desc").setLike("*描述" + houzhui++ + "*"));
                    //time += 10;
                    //con.addConditionSubList(new ConditionSub().setColumn("desc").setSearch("这是一个描述999999"));
                    //con.addConditionSubList(new ConditionSub().setColumn("name").setSearch("name"));
                    //con.addConditionSubList(new ConditionSub().setColumn("menu").setSearch("menu" + r.nextInt(9)));
                    //con.addConditionSubList(new ConditionSub().setColumn("menu").setSearch("menu" + 0));
                    //con.addConditionSubList(new ConditionSub().setColumn("menu").setSearch("menu" + 88));
                    //con.addConditionSubList(new ConditionSub().setColumn("shuzi").addQueryRange(new QueryRange(-r.nextInt(5), r.nextInt(5))));
                    //con.addConditionSubList(new ConditionSub().setColumn("shuzi").addQueryRange(new QueryRange(-2, -1)));

                    /*//固定模式，会使用到查询缓存
                    con.addConditionSubList(new ConditionSub().setColumn("time").addQueryRange(new QueryRange(time, time + 100 * 1000)));
                    con.addConditionSubList(new ConditionSub().setColumn("menu").setSearch("menu7"));
                    con.addConditionSubList(new ConditionSub().setColumn("shuzi").addQueryRange(new QueryRange(-1, 0)));*/

                    //con.addConditionSubList(new ConditionSub().setColumn("extendLong").setSearch(exLong));
                    //con.addConditionSubList(new ConditionSub().setColumn("name").setSearch("name"));
                    //con.addConditionSubList(new ConditionSub().setColumn("shuzi").setSearch(-2));
                    //con.addConditionSubList(new ConditionSub().setColumn("menu").setSearch("menu7"));
                    //con.addConditionSubList(new ConditionSub().setColumn("shuzi").addQueryRange(new QueryRange(-3, -2)));

                    /*con.addId(38).addId(238).addId(338).addId(1038).addId(1138).addId(1238)
                            .addId(1438).addId(1638).addId(1738).addId(2038);*/

                    long s = System.currentTimeMillis();
                    DataResult dataResult = BigstoreService.query(con);
                    //System.out.println("query-time: " + (System.currentTimeMillis() - s));

                    //打印结果数据
                    long count = 0;
                    if (dataResult.getDatas() != null) {
                        String column = "";
                        String sp = "";
                        for (String col : dataResult.getColumns()) {
                            column += sp + col;
                            sp = ", ";
                        }
                        System.out.println(column);
                        int countAdd = 0;
                        for (List<String> ls : dataResult.getDatas()) {
                            //System.out.println(ls.toString());
                            //count += CommonUtil.parseLong(ls.get(1));
                            if (countAdd++ > 10) {
                                break;
                            }
                        }
                    }
                    //System.out.println("total count:" + count);

                    System.out.println(i + ":query-time:" + (System.currentTimeMillis() - s) + ", total:"+ dataResult.getTotal() + ", size:" + dataResult.getDatas().size());
                    //Thread.sleep(1000L);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("time:" + (System.currentTimeMillis() - start));
        /*if (type == 1 || type == 2 || type == 10 || type == 3) {
            Thread.sleep(36000 * 1000);   //等待创建索引
        }*/
        System.exit(0);

    }

    private void test() {
        String line = "itemcenter|0A446817AC112EAD589BD871000399E0|739830689675947693|6384934876570950112|1487142192000|0|1|2886807212|0|{\"itemId:\":111}|xxxx|新增商品|111|222|333||||||||||||||||||sss1|3540094|sss2|3540095||";
        String srcFile = "/tmp/bigstore/src.txt";
        List<String> lines = new ArrayList<String>();
        for (int i = 0; i < 1000000; i++) {
            if (i % 100000 == 0) {
                System.out.println(i);
                FileUtil.writeFile(srcFile, lines, true);
            }
            lines.add(line);
        }
    }

    public static List<String[]> createLogDataBySplit() {
        int count = 100001;
        //int count = 10;
        List<String[]> lines = new ArrayList<String[]>();
        Random ran = new Random();
        //String split = Constants.COLUMN_SPLIT_2;
        for (int i = 0; i < count; i++) {
            int k = 0;
            String[] strArray = new String[9];
            strArray[k++] = UUID.randomUUID().toString();
            strArray[k++] = String.valueOf(i == 0 ? System.currentTimeMillis() / 100000 : System.currentTimeMillis() + ran.nextInt(100));
            strArray[k++] = "name";
            strArray[k++] = "这是一个描述" + i;
            if (ran.nextBoolean()) {
                strArray[k++] = null;
                strArray[k++] = null;
            } else {
                strArray[k++] = "extend" + ran.nextInt(100000000);
                strArray[k++] = String.valueOf(i);
                if (i == 100) {
                    totalByMo++;
                }
            }
            if (ran.nextBoolean()) {
                strArray[k++] = "menu" + (i % 10);
            } else {
                strArray[k++] = null;
            }
            strArray[k++] = String.valueOf(ran.nextInt(100) - 50);
            strArray[k++] = String.valueOf(ran.nextInt(1000) / 100.0F);
            lines.add(strArray);
        }
        return lines;
    }

    public static List<String> createLogData() {
        int count = 100001;
        //int count = 10;
        List<String> lines = new ArrayList<String>();
        Random ran = new Random();
        for (int i = 0; i < count; i++) {
            String log = System.currentTimeMillis() + "|" + "name" + "|" + "这是一个描述" + i;
            if (ran.nextBoolean()) {
                log += "|" + "|";
            } else {
                log += "|extend" + "|" + (i);
            }
            if (ran.nextBoolean()) {
                log += "|" + "menu" + (i % 10);
            } else {
                log += "|";
            }
            log += "|" + (ran.nextInt(100) - 50);
            lines.add(log);
        }
        return lines;
    }

    public static List<String> createLogData2() throws Exception {
        List<String> list = FileUtil.readAll("/Users/wuqing/Downloads/apollo_request", false);
        int count = list.size();
        List<String> lines = new ArrayList<String>();
        Random ran = new Random();
        for (int i = 0; i < count; i++) {
            //String log = System.currentTimeMillis() + "|" + "name" + "|" + "这是一个描述" + i;
            String log = System.currentTimeMillis() + "|" + "name" + "|" + list.get(i);
            if (ran.nextBoolean()) {
                log += "|" + "|";
            } else {
                log += "|extend" + "|" + (i);
            }
            if (ran.nextBoolean()) {
                log += "|" + "menu" + (i % 10);
            } else {
                log += "|";
            }
            log += "|" + (ran.nextInt(100) - 50);
            lines.add(log);
        }
        return lines;
    }

    public static long[] createData() {
        int count = 1;
        long[] lines = new long[count];
        for (int i = 0; i < count; i++) {
            long time = System.currentTimeMillis();
            lines[i] = time;
        }
        return lines;
    }

    public static List<ColumnDef> createTableData() {
        List<ColumnDef> list = new ArrayList<ColumnDef>();
        list.add(new ColumnDef("uuid", ColumnDef.STRING_ORDER));
        list.add(new ColumnDef("time", ColumnDef.LONG_ORDER_TYPE, 13));
        list.add(new ColumnDef("name", ColumnDef.STRING_FIND, 10));
        list.add(new ColumnDef("desc", ColumnDef.TEXT_INDEX));
        list.add(new ColumnDef("extend", ColumnDef.STRING_ORDER));
        list.add(new ColumnDef("extendLong", ColumnDef.LONG_FIND, 10));
        list.add(new ColumnDef("menu", ColumnDef.STRING_MENU, 2));
        list.add(new ColumnDef("shuzi", ColumnDef.LONG_TYPE));
        list.add(new ColumnDef("dec", ColumnDef.DECIMA_TYPE, 0, 3));
        return list;
    }


}
