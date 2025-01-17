package com.wuqing.client.bigstore.tcp.encode;

import com.wuqing.client.bigstore.util.HessianUtil;
import com.wuqing.client.bigstore.util.SnappyUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * Created by wuqing on 16/11/16.
 */
public class MyDecode extends ByteToMessageDecoder {

    private static final int HEAD_LENGTH = 4;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < HEAD_LENGTH) {  //这个HEAD_LENGTH是我们用于表示头长度的字节数。  由于上面我们传的是一个int类型的值，所以这里HEAD_LENGTH的值为4.
            return;
        }
        //in.markReaderIndex();                  //我们标记一下当前的readIndex的位置
        int dataLength = in.readInt();       // 读取传送过来的消息的长度。ByteBuf 的readInt()方法会让他的readIndex增加4
        if (dataLength < 0) { // 我们读到的消息体长度为0，这是不应该出现的情况，这里出现这情况，关闭连接。
            ctx.close();
        }

        if (in.readableBytes() < dataLength) { //读到的消息体长度如果小于我们传送过来的消息长度，则resetReaderIndex. 这个配合markReaderIndex使用的。把readIndex重置到mark的地方
            //in.resetReaderIndex();
            //in.clear(); //放弃此请求
            in.readBytes(new byte[in.readableBytes()]); //由于解码器会丢弃分隔符，所以就算将其resetReaderIndex 也会丢部分数据，无法反序列化，那么就直接读出来，丢弃好了
            return;
        }

        byte[] body = new byte[dataLength];  //  嗯，这时候，我们读到的长度，满足我们的要求了，把传送过来的数据，取出来吧~~
        in.readBytes(body);  //
        byte[] uncompress = SnappyUtil.decompress(body);
        if (uncompress != null) {
            body = uncompress;
        }
        Object o = HessianUtil.unserialize(body);  //将byte数据转化为我们需要的对象。伪代码，用什么序列化，自行选择
        out.add(o);
    }
}
