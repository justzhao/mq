package com.zhaopeng.mq.api.impl;

import com.zhaopeng.mq.api.service.SendService;
import com.zhaopeng.remoting.netty.NettyClient;
import com.zhaopeng.remoting.protocol.RemotingCommand;

/**
 * Created by zhaopeng on 2017/6/16.
 */
public class SendServiceImpl implements SendService {


    private final NettyClient nettyClient;

    public SendServiceImpl(NettyClient nettyClient) {
        this.nettyClient = nettyClient;
    }


    @Override
    public void sendMessage(RemotingCommand remotingCommand, long timeout) {

    }
}
