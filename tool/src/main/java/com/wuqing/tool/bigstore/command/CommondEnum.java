package com.wuqing.tool.bigstore.command;

/**
 * Created by wuqing on 17/1/9.
 */
public enum CommondEnum {
    /**
     * help命令
     */
    HELP("help", "introduce: get help to use"),

    /**
     * 连接数据库命令
     */
    CONNECT("connect", "format: connect [ip]:[port]\nexample: connect 127.0.0.1\nexample: connect 127.0.0.1:60000"),

    /**
     * 展示所有数据库列表
     */
    SHOW_DATABASES("show databases", "format: show databases\nintroduce: show all databases in DB"),

    /**
     * 展示当前数据库下的所有表
     */
    SHOW_TABLES("show tables", "format: show tables\nintroduce: show all tables in database"),

    /**
     * 使用某个数据库
     */
    USE("use", "format: use [database]\nexample: use test_database"),

    /**
     * 展示表结构
     */
    DESC("desc", "format: desc [table]\nintroduce: desc test_table"),

    /**
     * 查询语句
     */
    SELECT("select", "format: select [field] from [table] where [filed] between [low] and [high] limit [start], [limit] "
            + "\nexample: select * from test_table where time between 1528100135 and 1528105135 limit 1, 10"
            + "\nexample: select time, name, old from test_table where time = 1528103135 limit 10"
            + "\nexample: select avg(time), sum(time), min(time), max(time) from test_table where time between 1528100135 and 1528105135"
            + "\nexample: select * from test_table where msg search 'wordkey' "
            + "\nexample: select * from test_table where msg grep 'wordkey' "),

    /**
     * 退出
     */
    EXIT("exit", "introduce: exit process");


    private String name;

    private String desc;

    private CommondEnum(String name, String desc) {
        this.name = name;
        this.desc = desc;
    }

    public String getName() {
        return this.name;
    }

    public String getDesc() {
        return desc;
    }

    public static String[] getNameAll() {
        int length = CommondEnum.values().length;
        String[] names = new String[length];
        int i = 0;
        for (CommondEnum c : CommondEnum.values()) {
            names[i++] = c.getName();
        }
        return names;
    }
}
