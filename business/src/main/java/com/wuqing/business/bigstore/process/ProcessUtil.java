package com.wuqing.business.bigstore.process;

import com.wuqing.business.bigstore.bean.ProcessResult;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuqing on 16/9/20.
 */
public class ProcessUtil {

    private final static Logger logger = LoggerFactory.getLogger(ProcessUtil.class);
    private final static Logger compresslogger = LoggerFactory.getLogger("compress-log");
    private final static Logger sendDatalogger = LoggerFactory.getLogger("send-data-log");


    public static ProcessResult waitDestroy(Process p) throws InterruptedException {
        ProcessResult proResult = new ProcessResult();
        List<String> result = new ArrayList<String>();
        StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), "Error");
        StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), "Output");
        errorGobbler.start();
        outputGobbler.start();
        /*while (!(errorGobbler.isComplete() && outputGobbler.isComplete())) {
            Thread.currentThread().sleep(10L);
        }*/
        p.waitFor();
        //p.destroy();
        for (int i = 0, k = outputGobbler.getLineList().size(); i < k; i++) {
            String info = outputGobbler.getLineList().get(i);
            if (StringUtils.isNotBlank(info)) {
                result.add("[INFO]" + info);
            }

        }
        boolean success = true;
        for (int i = 0, k = errorGobbler.getLineList().size(); i < k; i++) {
            String error = errorGobbler.getLineList().get(i);
            if (StringUtils.isNotBlank(error)) {
                result.add("[ERROR]" + error);
                success = false;
            }
        }
        proResult.setResList(result);
        proResult.setSuccess(success);
        return proResult;
    }

    public static ProcessResult execute(String commond) {
        try {
            String[] cmds = new String[]{"/bin/bash", "-c", commond};
            ProcessBuilder builder = new ProcessBuilder(cmds);
            File out = new File("out.log");
            if (!out.exists()) {
                out.createNewFile();
            }
            //builder.redirectError(out);
            builder.redirectOutput(out);
            Process p = builder.start();
            //Process p = Runtime.getRuntime().exec(cmds);
            return ProcessUtil.waitDestroy(p);
        } catch (Exception e) {
            logger.error("execute process fail.", e);
        }
        return null;
    }

    /**
     * 进行数据主从同步
     * @param dp data pack 完整路径
     * @return
     */
    public static String executeScpSync(String dp, String ip, boolean syncDpData) {
        long s = System.currentTimeMillis();
        List<String> list = Commond.getSyncScpCommond(dp, ip, syncDpData);
        sendDatalogger.debug("sync data, path:" + dp);
        boolean success = true;
        int i = 0;
        for (String cmd : list) {
            sendDatalogger.debug("execute commond by bash:" + cmd);
            ProcessResult processResult = execute(cmd);
            if (i++ > 0) {
                success &= processResult.isSuccess();
            }
            StringBuilder sb = new StringBuilder();
            sb.append("success:").append(processResult.isSuccess()).append("\n");
            for (String l : processResult.getResList()) {
                sb.append(l).append("\n");
            }
            String msg = null;
            if (sb.length() > 0) {
                msg = sb.substring(0, sb.length() - 1);
            }
            sendDatalogger.debug("sync data, result: " + msg);
        }
        sendDatalogger.debug("sync data, total result: " + success + ", time:" + (System.currentTimeMillis() - s));
        return "success:" + success;
    }


    /**
     * 进行数据主从同步
     * @param dp data pack 完整路径
     * @return
     */
    public static String executeSync(String dp, String ip) {
        long s = System.currentTimeMillis();
        String cmd = Commond.getSyncCommond(dp, ip);
        sendDatalogger.debug("sync data, path:" + dp);
        sendDatalogger.debug("execute commond by bash:" + cmd);
        ProcessResult processResult = execute(cmd);
        StringBuilder sb = new StringBuilder();
        sb.append("success:").append(processResult.isSuccess()).append("\n");
        for (String l : processResult.getResList()) {
            sb.append(l).append("\n");
        }
        String msg = null;
        if (sb.length() > 0) {
            msg = sb.substring(0, sb.length() - 1);
        }
        sendDatalogger.debug("sync data, result: " + msg + ", time:" + (System.currentTimeMillis() - s));
        return msg;
    }


    public static String executeDelDataFile(String dpPaht) {
        String cmd = Commond.getDelDataFileCommond(dpPaht);
        compresslogger.debug("execute commond by bash:" + cmd);
        ProcessResult processResult = execute(cmd);
        StringBuilder sb = new StringBuilder();
        sb.append("success:").append(processResult.getResList().isEmpty()).append("\n");
        for (String l : processResult.getResList()) {
            sb.append(l).append("\n");
        }
        if (sb.length() > 0) {
            return sb.substring(0, sb.length() - 1);
        } else {
            return null;
        }
    }

    public static void main(String[] args) {
        /*String result = executeSync("/tmp/bigstore/default_data_base/test_table/rows/0000000002/0000000293", "10.17.32.162");
        System.out.println(result);*/
        for (int i = 0; i < 10000; i++) {
            //String res = executeDelDataFile("/tmp/bigstore/default_data_base/test_table/rows/0000000009/0000000900");
            long s = System.currentTimeMillis();
            //String result = executeSync("/tmp/bigstore/default_data_base/test_table/rows/0000000002/0000000293", "10.17.32.162");
            String result = executeScpSync("/tmp/bigstore/default_data_base/test_table/rows/0000000002/0000000293", "10.17.32.162", true);
            long time = System.currentTimeMillis() - s;
            if (time > 100) {
                System.out.println(i + ":" + result + ":" + time);
            }
        }
        /*ProcessResult resList = execute("ls ~*//*");
        System.out.println(resList.getResList());*/
    }

}
