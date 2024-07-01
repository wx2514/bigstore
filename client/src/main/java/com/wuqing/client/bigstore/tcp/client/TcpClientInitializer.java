package com.wuqing.client.bigstore.tcp.client;

import com.wuqing.client.bigstore.bean.pkg.ResultListener;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.tcp.encode.MyDecode;
import com.wuqing.client.bigstore.tcp.encode.MyEncode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;

import java.util.Map;

public class TcpClientInitializer extends ChannelInitializer<SocketChannel> {


    private TcpClientHandler tcpClientHandler;

    public TcpClientInitializer(TcpClient client) {
        this.tcpClientHandler = new TcpClientHandler(client);
    }

    /*public void stop() {
        this.tcpClientHandler.stop();
    }*/

    /*public Object getResult(long pkgTime, long  timeout) {
        return tcpClientHandler.getResult(pkgTime, timeout);
    }*/

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        /*
         * 这个地方的 必须和服务端对应上。否则无法正常解码和编码
         */
        ByteBuf[] sp = new ByteBuf[] {Unpooled.wrappedBuffer(Constants.SPLIT)};
        pipeline.addLast("framer", new DelimiterBasedFrameDecoder(Constants.MAX_FRAME, sp));
        /*pipeline.addLast("decoder", new StringDecoder());
        pipeline.addLast("encoder", new StringEncoder());*/
        pipeline.addLast("decoder", new MyDecode());
        pipeline.addLast("encoder", new MyEncode());

        // 客户端的逻辑
        pipeline.addLast("handler", tcpClientHandler);
    }

    public void addResultListener(long seq, ResultListener listener) {
        this.tcpClientHandler.addResultListener(seq, listener);
    }

    public Map<Long, ResultListener> getResultListeners() {
        return this.tcpClientHandler.getResultListeners();
    }
}