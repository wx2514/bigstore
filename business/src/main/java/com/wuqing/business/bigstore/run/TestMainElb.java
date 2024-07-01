package com.wuqing.business.bigstore.run;

import com.wuqing.business.bigstore.service.BigstoreService;
import com.wuqing.client.bigstore.bean.Condition;
import com.wuqing.client.bigstore.bean.DataResult;

/**
 * Created by wuqing on 17/2/17.
 * 起始函数
 */
public class TestMainElb {

    private static int type = 3;

    public static void main(String[] args) throws Exception {
        Condition con = new Condition("elb");
        con.setSql("select scount(time_iso8601) from topic-elb-pro where host likeor ('web-tools-api.idanchuang.com','web-tools-api.danchuangglobal.com') and elb_name='loadbalancer_0674c9c8-288d-4317-887c-0a4f4b649cfe' group by status");
        DataResult dataResult = BigstoreService.query(con);
        System.exit(0);

    }




}
