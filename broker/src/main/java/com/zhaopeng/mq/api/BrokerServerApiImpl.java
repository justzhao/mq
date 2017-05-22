package com.zhaopeng.mq.api;

import com.zhaopeng.remoting.netty.NettyServer;

/**
 * Created by zhaopeng on 2017/5/22.
 */
public class BrokerServerApiImpl {

    private final NettyServer nettyServer;

    private final String addr;

    public BrokerServerApiImpl(NettyServer nettyServer, String addr) {
        this.nettyServer = nettyServer;

        this.addr = addr;
    }
}
