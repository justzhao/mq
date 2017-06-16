package com.zhaopeng.mq.processor;

import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by zhaopeng on 2017/6/16.
 */
public class BrokerClientProcessor implements NettyRequestProcessor {
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        return this.processRequest(ctx.channel(),request);
    }

    private  RemotingCommand processRequest(final Channel channel, RemotingCommand request){

        int code=request.getCode();

        switch (code){

        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
