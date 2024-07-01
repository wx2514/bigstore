package com.wuqing.client.bigstore.tcp.encode;

import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.client.bigstore.util.HessianUtil;
import com.wuqing.client.bigstore.util.SnappyUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by wuqing on 16/11/16.
 */
public class MyEncode extends MessageToByteEncoder {

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        byte[] body = HessianUtil.serialize(msg);  //将对象转换为byte，
        body = SnappyUtil.compress(body);
        int dataLength = body.length;  //读取消息的长度
        out.writeInt(dataLength);  //先将消息长度写入，也就是消息头
        out.writeBytes(body);
        out.writeBytes(Constants.SPLIT);
    }
}
