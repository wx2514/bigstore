package com.wuqing.client.bigstore.tcp.client;

import com.wuqing.client.bigstore.bean.*;
import com.wuqing.client.bigstore.bean.pkg.FutureResult;
import com.wuqing.client.bigstore.bean.pkg.ResultListener;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.hold.QueryTableHolder;
import com.wuqing.client.bigstore.bean.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class TcpClient {

    private static final int WAIT_CONNECT_STS = 0;

    private static final int CONNECTING_STS = 1;

    private static final int STOP_STS = -1;

    private TcpClientInitializer tcpClientInitializer;
;
    private EventLoopGroup group;

    private String host;

    private int port;

    private Channel ch;

    private int run = 0;

    public TcpClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.startClient(); //默认启动
    }

    public void startClient() {
        //System.out.println("start client, ip:" + this.host + ", port:" + port);
        if (run == STOP_STS) {
            return; //已被终止的无法重连
        }
        if (group != null) {    //如果是重连的，则先关闭上一次的group
            group.shutdownGracefully();
        }
        /*if (tcpClientInitializer != null) {
            tcpClientInitializer.stop();
        }*/
        try {
            Bootstrap b = new Bootstrap();
            tcpClientInitializer = new TcpClientInitializer(this);
            group = new NioEventLoopGroup(1);
            b.group(group).channel(NioSocketChannel.class)
                    .handler(tcpClientInitializer);
            // 连接服务端
            ch = b.connect(host, port).sync().channel();
            run = CONNECTING_STS;
        } catch (Exception e) { //连接失败
            run = WAIT_CONNECT_STS;
            //e.printStackTrace();
            System.err.println("tcp client start fail. exception:" + e.getMessage());
        }
    }

    /*public Object getResult(Long time, long timeout) {
        if (!isActive()) {  //如果连接失败，或者连接不是存活的，直接返回失败
            return null;
        }
        return tcpClientInitializer.getResult(time, timeout);
    }*/

    /*public Object getResult(Long time) {
        return getResult(time, 0L);
    }*/

    public ChannelFuture writeAndFlush(MyPackage pkg) {
        if (run == WAIT_CONNECT_STS) { //如果还没有启动成功，重启
            this.startClient();
        }
        if (run != CONNECTING_STS) { //没有连接成功
            return null;
        }
        return ch.writeAndFlush(pkg);
    }

    public void stopClient() {
        //System.out.println("stop client, ip:" + this.host + ", port:" + port);
        if (group != null) {
            group.shutdownGracefully();
        }
        /*if (tcpClientInitializer != null) {
            tcpClientInitializer.stop();
        }*/
        run = STOP_STS;
    }

    public boolean isShutdown() {
        return group.isShutdown();
    }

    /**
     * 判定连接是否存活
     * @return
     */
    public boolean isActive() {
        return ch != null && ch.isActive() && ch.isOpen();
    }

    public void addResultListener(long seq, ResultListener listener) {
        this.tcpClientInitializer.addResultListener(seq, listener);
    }

    public Map<Long, ResultListener> getResultListeners() {
        return this.tcpClientInitializer.getResultListeners();
    }

    /**
     * @param args
     * @throws InterruptedException
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {
        TcpClient client = new TcpClient("127.0.0.1", Constants.PORT);
        Random r = new Random();
        long time = 1491810735818L;
        for (int ii = 0; ii < 1; ii++) {
            long s = System.currentTimeMillis();
            MyPackage p = new MyPackage();
            Condition con = new Condition(Constants.DEFAULT_DATA_BASE);
            con.setTable("test_table");
            con.setLimit(10);
            con.setStart(0);
            con.addConditionSubList(new ConditionSub().setColumn("time").addQueryRange(new QueryRange(time, time + 100 * 1000)));
            //con.addConditionSubList(new ConditionSub().setColumn("menu").setSearch("menu" + r.nextInt(50)));
            p.setData(new QueryTableHolder(con));
            client.writeAndFlush(p);
            ResultListener<ResponseData> listener = new FutureResult<ResponseData>(10);
            client.addResultListener(p.getTime(), listener);
            ResponseData res = listener.getResult();
            if (res.isSuccess() && res.getData() != null) {
                DataResult result = (DataResult) res.getData();
                while (result.next()) {
                    for (int i = 0; i < 8; i++) {
                        System.out.print(result.getString(i) + " ");
                    }
                    System.out.println();
                }
            }
            time += 10L;
            System.out.println("time:" + (System.currentTimeMillis() - s));
            System.out.println();
        }
        client.stopClient();
    }

}