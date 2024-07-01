package com.wuqing.business.bigstore.tcp.server;


import com.wuqing.client.bigstore.util.CommonUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpServer {

    private static final Logger logger = LoggerFactory.getLogger(TcpServer.class);

    private EventLoopGroup bossGroup = null;
    private EventLoopGroup workerGroup = null;
    private ChannelFuture f = null;
    private int port;

    public TcpServer(int port) {
        this.port = port;
    }

    public void startServer() throws Exception {
        logger.info("start server, port:" + this.port);
        //一个监听端口，设置成1好了
        bossGroup = new NioEventLoopGroup(1);
        //默认是 CPU * 2 个 work 线程
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup);
        b.channel(NioServerSocketChannel.class);
        b.childHandler(new TcpServerInitializer());
        // 服务器绑定端口监听
        f = b.bind(this.port).sync();
        // 监听服务器关闭监听
        //f.channel().closeFuture().sync();
    }

    public boolean isShutdown() {   //有一个停止了就认为已经关闭了，或者即将关闭
        return bossGroup.isShutdown() || workerGroup.isShutdown();
    }

    public void stopServer() {
        logger.info("stop server, port:" + this.port);
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (f != null && f.channel() != null) {
            f.channel().close();
        }
    }

    public void awaitStop() {
        long start = System.currentTimeMillis();
        //因为指令也是由于worker发送的，所以这时候不可能被优雅的关闭
        while (System.currentTimeMillis() - start < 60000) {    //最多等待60秒
            if (bossGroup.isShutdown() && workerGroup.isShutdown()) {
                break;
            } else {
                CommonUtil.sleep(1000L);
            }
        }
    }

    public static void main(String[] args) throws Exception {

    }
}