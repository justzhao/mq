package com.zhaopeng.remoting.netty;

import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.net.InetSocketAddress;

/**
 * Created by zhaopeng on 2017/3/23.
 */
public class NettyServer implements Server {


    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGroupSelector;
    private final EventLoopGroup eventLoopGroupBoss;
    private final NettyServerConfig nettyServerConfig;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    public NettyServer(ServerBootstrap serverBootstrap, EventLoopGroup eventLoopGroupSelector, EventLoopGroup eventLoopGroupBoss, NettyServerConfig nettyServerConfig) {
        this.serverBootstrap = new ServerBootstrap();
        this.eventLoopGroupSelector = eventLoopGroupSelector;
        this.eventLoopGroupBoss = eventLoopGroupBoss;
        this.nettyServerConfig = nettyServerConfig;
    }

    public void start() {
        this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector).channel(NioServerSocketChannel.class)
                //
                .option(ChannelOption.SO_BACKLOG, 1024)
                //
                .option(ChannelOption.SO_REUSEADDR, true)
                //
                .option(ChannelOption.SO_KEEPALIVE, false)
                //
                .childOption(ChannelOption.TCP_NODELAY, true)
                //
                .option(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize())
                //
                .option(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize())
                //
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {

                        ch.pipeline().addLast(defaultEventExecutorGroup,
                                new NettyEncoder(), //
                                new NettyDecoder(), //
                                new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                new NettyServerHandler());
                    }
                });

    }

    public void shutdown() {

    }

    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
          //  processMessageReceived(ctx, msg);
        }
    }


}
