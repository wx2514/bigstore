package com.wuqing.business.bigstore.process;

import com.wuqing.client.bigstore.util.CommonUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 17/8/16.
 */
public class Commond {

    //private static String DATA_COPY_CMD = "/usr/bin/rsync -RP ${dp}/* ${ds}/*.txt ${tb}/*.txt dahong@${ip}::dahonghome --password-file=/etc/rsync.secret";
    //private static String DATA_COPY_CMD = "/usr/bin/rsync -RP ${dp}/*.*t ${dp}/*.*z ${ds}/*_space.txt ${tb}/*.txt dahong@${ip}::dahonghome --password-file=/etc/rsync.secret";
    /**
     * 这样的同步方式，目前只支持 异步压缩模式。不支持同步压缩文件。
     */
    private static String DATA_COPY_CMD = "/usr/bin/rsync -qRP ${dp}/*.*t ${ds}/*_space.txt ${tb}/*.txt dahong@${ip}::dahonghome --password-file=/etc/rsync.secret";

    private static String SCP_0 = "ssh -p10022 ${ip} 'mkdir -p ${dp}'";
    private static String SCP_1 = "scp -P10022 ${dp}/*.*t ${ip}:${dp}/";
    private static String SCP_2 = "scp -P10022 ${ds}/*_space.txt ${ip}:${ds}/";
    private static String SCP_3 = "scp -P10022 ${tb}/*.txt ${ip}:${tb}/";

    public static List<String> getSyncScpCommond(String dp, String ip, boolean syncDpData) {
        if (CommonUtil.isEmpty(dp)) {
            return null;
        }
        if (dp.endsWith("/")) {
            dp = dp.substring(0, dp.length() - 1);
        }
        String ds = dp.substring(0, dp.lastIndexOf("/"));
        String tb = ds.substring(0, ds.lastIndexOf("/"));
        tb = tb.substring(0, tb.lastIndexOf("/"));
        List<String> commonds = new ArrayList<String>();
        commonds.add(SCP_0.replace("${dp}", dp).replace("${ip}", ip));
        if (syncDpData) {
            commonds.add(SCP_1.replace("${dp}", dp).replace("${ip}", ip));
        }
        commonds.add(SCP_2.replace("${ds}", ds).replace("${ip}", ip));
        commonds.add(SCP_3.replace("${tb}", tb).replace("${ip}", ip));
        return commonds;
    }

    public static String getSyncCommond(String dp, String ip) {
        if (CommonUtil.isEmpty(dp)) {
            return null;
        }
        if (dp.endsWith("/")) {
            dp = dp.substring(0, dp.length() - 1);
        }
        String ds = dp.substring(0, dp.lastIndexOf("/"));
        String tb = ds.substring(0, ds.lastIndexOf("/"));
        tb = tb.substring(0, tb.lastIndexOf("/"));
        return DATA_COPY_CMD.replace("${dp}", dp).replace("${ds}", ds).replace("${tb}", tb).replace("${ip}", ip);
    }

    public static String getDelDataFileCommond(String path) {
        return "rm -f " + path + "/0*/*.txt";
    }

    public static void main(String[] args) {
        String dp = "/block1/data/lurker/lurker_20170813_2_0/rows/0000000075/0000007596";
        String cmd = getSyncCommond(dp, "10.17.32.162");
        System.out.println(cmd);
    }
}
