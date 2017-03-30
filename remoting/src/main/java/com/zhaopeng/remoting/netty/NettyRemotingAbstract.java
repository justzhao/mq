package com.zhaopeng.remoting.netty;

import com.zhaopeng.remoting.protocol.RemotingCommand;
import com.zhaopeng.remoting.protocol.RemotingCommandType;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by zhaopeng on 2017/3/26.
 */
public class NettyRemotingAbstract {


    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            switch (cmd.getType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 处理request
     *
     * @param ctx
     * @param cmd
     */
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {

        final String requestId = cmd.getRequestId();
        Runnable run = new Runnable() {
            public void run() {
                 RemotingCommand response=new RemotingCommand();
                if(!cmd.isOneWay()){
                    response.setRequestId(requestId);
                    response.setType(RemotingCommandType.RESPONSE_COMMAND);
                    ctx.writeAndFlush(response);
                }

            }
        };




    }

    /**
     * 处理respone
     *
     * @param ctx
     * @param cmd
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {

    }
}
