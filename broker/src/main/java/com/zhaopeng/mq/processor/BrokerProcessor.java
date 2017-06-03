package com.zhaopeng.mq.processor;

import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import static com.zhaopeng.common.protocol.RequestCode.PULL_MESSAGE;
import static com.zhaopeng.common.protocol.RequestCode.SEND_MESSAGE;

/**
 * Created by zhaopeng on 2017/5/21.
 */
public class BrokerProcessor implements NettyRequestProcessor {
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        return this.processRequest(ctx.channel(),request);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private  RemotingCommand processRequest(final Channel channel, RemotingCommand request){

        int code=request.getCode();

        switch (code){
            case PULL_MESSAGE :{

            }
            case SEND_MESSAGE:{

            }
        }
        return null;
    }
}
