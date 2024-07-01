package com.wuqing.business.bigstore.tcp.server;

import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.tcp.encode.MyDecode;
import com.wuqing.client.bigstore.tcp.encode.MyEncode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;

public class TcpServerInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        ByteBuf[] sp = new ByteBuf[] {Unpooled.wrappedBuffer(Constants.SPLIT)};
        pipeline.addLast("framer", new DelimiterBasedFrameDecoder(Constants.MAX_FRAME, sp));

        // 字符串解码 和 编码
        /*pipeline.addLast("decoder", new StringDecoder());
        pipeline.addLast("encoder", new StringEncoder());*/
        pipeline.addLast("decoder", new MyDecode());
        pipeline.addLast("encoder", new MyEncode());

        // 自己的逻辑Handler
        pipeline.addLast("handler", new TcpServerHandler());
    }
}