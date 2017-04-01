package com.zhaopeng.remoting.netty;

import com.zhaopeng.remoting.common.Pair;
import com.zhaopeng.remoting.common.ServiceThread;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import com.zhaopeng.remoting.protocol.RemotingCommandType;
import com.zhaopeng.remoting.protocol.RemotingSysResponseCode;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Created by zhaopeng on 2017/3/26.
 */
public class NettyRemotingAbstract {
    private static final Logger logger = LoggerFactory.getLogger(NettyRemotingAbstract.class);

    // 用来存放发出去的request
    protected final ConcurrentHashMap<String /* requestId */, ResponseFuture> responseTable =
            new ConcurrentHashMap<>(256);


    // 根据请求的业务类型存放的业务处理器
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable = new HashMap<>(64);

    protected final NettyEventExecuter nettyEventExecuter = new NettyEventExecuter();

    //默认的业务处理器
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;


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

        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessor : matched;
        final String requestId = cmd.getRequestId();
        if (pair != null) {
            Runnable run = new Runnable() {
                public void run() {
                    try {
                        RemotingCommand response = pair.getObject1().processRequest(ctx, cmd);
                        if (!cmd.isOneWay()) {
                            response.setRequestId(requestId);
                            response.setType(RemotingCommandType.RESPONSE_COMMAND);
                            ctx.writeAndFlush(response);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            };

            pair.getObject2().submit(run);


        } else {
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setRequestId(requestId);
            ctx.writeAndFlush(response);

            logger.error(ctx.channel() + error);
        }

    }

    /**
     * 处理respone
     *
     * @param ctx
     * @param cmd
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {

    }

    class NettyEventExecuter extends ServiceThread {

        public String getServiceName() {
            return null;
        }

        public void run() {

        }
    }
}
