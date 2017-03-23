package com.zhaopeng.remoting.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;

/**
 * Created by zhaopeng on 2017/3/23.
 */
public class NettyServer implements Server {



    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGroupSelector;
    private final EventLoopGroup eventLoopGroupBoss;
   // private final NettyServerConfig nettyServerConfig;

    public NettyServer(ServerBootstrap serverBootstrap, EventLoopGroup eventLoopGroupSelector, EventLoopGroup eventLoopGroupBoss) {
        this.serverBootstrap = serverBootstrap;
        this.eventLoopGroupSelector = eventLoopGroupSelector;
        this.eventLoopGroupBoss = eventLoopGroupBoss;
    }

    public void start() {

    }

    public void shutdown() {

    }
}
