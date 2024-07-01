package com.wuqing.ui.bigstore.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class UiServer {

    private int port;

    public UiServer(int port) {
        this.port = port;
    }

    public void startServer() throws Exception {
        NioEventLoopGroup boss = new NioEventLoopGroup(1);
        NioEventLoopGroup work = new NioEventLoopGroup(2);

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(boss, work)
                .channel(NioServerSocketChannel.class)
                .childHandler(new HttpServerInitializer())
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
        ChannelFuture future = serverBootstrap.bind(port).sync();
        // 等待服务器  socket 关闭 。
        // 在这个例子中，这不会发生，但你可以优雅地关闭你的服务器。
        //future.channel().closeFuture().sync();

    }

}
