package com.zhaopeng.mq.consumer.impl;

import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by zhaopeng on 2017/4/30.
 */
public class ClientRemotingProcessor implements NettyRequestProcessor {
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
