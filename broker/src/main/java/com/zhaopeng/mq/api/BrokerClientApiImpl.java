package com.zhaopeng.mq.api;

import com.zhaopeng.remoting.netty.NettyClient;

/**
 * Created by zhaopeng on 2017/5/22.
 */
public class BrokerClientApiImpl {

    private final NettyClient nettyClient;

    private final String addr;

    public BrokerClientApiImpl(NettyClient nettyClient, String addr) {
        this.nettyClient = nettyClient;
        this.addr = addr;
    }
}
