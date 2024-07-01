package com.wuqing.client.bigstore.hold;

import java.io.Serializable;

/**
 * Created by wuqing on 17/4/5.
 */
public class Holder implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 查询数据
     */
    public static final int QUERY = 0;

    /**
     * 创建表
     */
    public static final int CREATE = 1;

    /**
     * 本地load
     */
    public static final int LOAD_DATA = 2;

    /**
     * 批量保存
     */
    public static final int STORE_DATA = 3;

    /**
     * 异步保存
     */
    public static final int ASYNC_STORE_DATA = 4;

    /**
     * 异步批量保存
     */
    public static final int ASYNC_BATCH_STORE_DATA = 5;

    /**
     * 显示所有的数据库
     */
    public static final int SHOW_DATABASES = 10;

    /**
     * 显示所有的数据表
     */
    public static final int SHOW_TABLES = 11;

    /**
     * 显示表字段
     */
    public static final int DESC_TABLES = 12;

    /**
     * 刷新表缓存
     */
    public static final int FLUSH_TABLE_CACHE = 13;

    /**
     * 刷新表缓存
     */
    public static final int ADD_TABLE_COLUMN = 14;



    /**
     * 0: 查询表 实现类对应 QueryTableHolder
     * 1: 创建表 实现类对应 CreateTableHolder
     * 2: 批量保存数据（本地load模式）实现类对应 LoadDataHolder
     * 3: 批量保存数据（远程store模式）实现类对应 StoreDataHolder
     */
    protected int type = 0;

    public int getType() {
        return type;
    }
}
