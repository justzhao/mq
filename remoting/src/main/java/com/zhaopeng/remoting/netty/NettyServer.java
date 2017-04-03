package com.zhaopeng.remoting.netty;

import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.Server;
import com.zhaopeng.remoting.common.Pair;
import com.zhaopeng.remoting.protocol.ChannelEventListener;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaopeng on 2017/3/23.
 */
public class NettyServer extends NettyRemotingAbstract implements Server {


    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGroupSelector;
    private final EventLoopGroup eventLoopGroupBoss;
    private final NettyServerConfig nettyServerConfig;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    private final ExecutorService publicExecutor;

    private final ChannelEventListener channelEventListener;


    public NettyServer(EventLoopGroup eventLoopGroupSelector, EventLoopGroup eventLoopGroupBoss, NettyServerConfig nettyServerConfig,ChannelEventListener channelEventListener) {
        super(10,10);
        this.serverBootstrap = new ServerBootstrap();
        this.eventLoopGroupSelector = eventLoopGroupSelector;
        this.eventLoopGroupBoss = eventLoopGroupBoss;
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener=channelEventListener;

        this.publicExecutor = Executors.newFixedThreadPool(4, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyServerPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });
    }

    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(//
                nettyServerConfig.getServerWorkerThreads(), //
                new ThreadFactory() {

                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyServerCodecThread_" + this.threadIndex.incrementAndGet());
                    }

                    private AtomicInteger threadIndex = new AtomicInteger(0);
                });


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

    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {

        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
        this.processorTable.put(requestCode, pair);

    }

    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair<>(processor, executor);
    }

    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return null;
    }

    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }


    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }
}
