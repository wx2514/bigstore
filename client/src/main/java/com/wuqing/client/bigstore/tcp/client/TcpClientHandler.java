package com.wuqing.client.bigstore.tcp.client;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.wuqing.client.bigstore.bean.MyPackage;
import com.wuqing.client.bigstore.bean.pkg.ResultListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.*;

public class TcpClientHandler extends SimpleChannelInboundHandler<Object> {

    private TcpClient client;

    private final Map<Long, ResultListener> listeners = new ConcurrentLinkedHashMap.Builder<Long, ResultListener>()
            .maximumWeightedCapacity(3000).build();     //最多并发获取3000个结果


    public TcpClientHandler(TcpClient client) {
        this.client = client;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg == null || !(msg instanceof MyPackage)) {
            System.err.println("client msg is invalid, msg:" + msg);
            return;
        }
        MyPackage pkg = (MyPackage) msg;
        ResultListener resultListener = listeners.remove(pkg.getTime());
        resultListener.setResult(pkg.getData());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        //System.out.println("Client active ");
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        //System.out.println("Client close ");
        this.client.startClient();  //重连
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        //System.out.println("client catch exception");
        cause.printStackTrace();
        //ctx.close();
    }

    public void addResultListener(long seq, ResultListener listener) {
        //System.out.println(seq);
        this.listeners.put(seq, listener);
    }

    public Map<Long, ResultListener> getResultListeners() {
        return this.listeners;
    }
}