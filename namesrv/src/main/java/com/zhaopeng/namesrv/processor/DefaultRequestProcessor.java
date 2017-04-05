package com.zhaopeng.namesrv.processor;

import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;


/**
 * Created by zhaopeng on 2017/4/5.
 */
public class DefaultRequestProcessor implements NettyRequestProcessor {


    /**
     *  根据request 中的业务操作码来实现 不同的业务类型
     * @param ctx
     * @param request
     * @return
     * @throws Exception
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {


        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
