package com.wuqing.client.bigstore.run;

import com.wuqing.client.bigstore.bean.MyPackage;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.tcp.client.TcpClient;

/**
 * Created by wuqing on 17/4/10.
 */
public class StopService {

    public static void main(String[] args) {
        TcpClient client = new TcpClient("127.0.0.1", Constants.PORT);
        MyPackage p = new MyPackage();
        p.setData("stop");
        client.writeAndFlush(p);
        client.stopClient();
    }

}
