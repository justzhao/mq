package com.zhaopeng.remoting;

import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by zhaopeng on 2017/3/31.
 *
 * 用来处理 通信过程中的业务逻辑，
 *
 */
public interface NettyRequestProcessor {
    /**
     * 处理业务消息
     * @param ctx
     * @param request
     * @return
     * @throws Exception
     */
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws Exception;

    /**
     * 不处理消息
     * @return
     */
    boolean rejectRequest();
}
