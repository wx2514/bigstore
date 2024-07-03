package com.wuqing.business.bigstore.run;

import com.wuqing.business.bigstore.cache.DataCache;
import com.wuqing.business.bigstore.config.Params;
import com.wuqing.business.bigstore.tcp.server.TcpServer;
import com.wuqing.business.bigstore.thread.CleanRunnable;
import com.wuqing.business.bigstore.thread.CompressRunnable;

/**
 * Created by wuqing on 17/2/17.
 * 起始函数
 */
public class StartMain2 {

    public static TcpServer server = null;

    public static void main(String[] args) throws Exception {
        Params.load("/params2.properties");
        server = new TcpServer(60001);
        server.startServer();
        CompressRunnable.start();
        CleanRunnable.start();
        DataCache.directCheck();
    }

}
