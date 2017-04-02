package com.zhaopeng.remoting.netty;

import com.zhaopeng.remoting.InvokeCallback;
import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.common.Pair;
import com.zhaopeng.remoting.common.ServiceThread;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import com.zhaopeng.remoting.protocol.RemotingCommandType;
import com.zhaopeng.remoting.protocol.RemotingSysResponseCode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
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
        String requestId = cmd.getRequestId();
        ResponseFuture responseFuture = responseTable.get(requestId);
        if (responseFuture != null) {

            responseFuture.setResponseCommand(cmd);
            responseTable.remove(requestId);

            if (responseFuture.getInvokeCallback() != null) {

                responseFuture.executeInvokeCallback();
            } else {
                responseFuture.putResponse(cmd);
            }


        } else {
            logger.error("receive response, but not matched any request, {}", ctx.channel());
        }

    }

    /**
     * 同步请求
     *
     * @param channel
     * @param request
     * @param timeoutMillis
     * @return
     */
    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingException {

        final String requestId = request.getRequestId();

        try {
            final ResponseFuture responseFuture;
            responseFuture = new ResponseFuture(requestId, timeoutMillis, null);
            this.responseTable.put(requestId, responseFuture);
            final SocketAddress addr = channel.remoteAddress();
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    } else {
                        responseFuture.setSendRequestOK(false);
                    }

                    responseTable.remove(requestId);
                    responseFuture.setCause(f.cause());
                    responseFuture.putResponse(null);
                    logger.warn("send a request command to channel <" + addr + "> failed.");
                }
            });

            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (null == responseCommand) {
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingException(addr.toString() + " " + timeoutMillis,
                            responseFuture.getCause());
                } else {
                    throw new RemotingException(addr.toString(), responseFuture.getCause());
                }
            }

            return responseCommand;
        } finally {
            this.responseTable.remove(requestId);
        }

    }


    /**
     * 异步请求
     *
     * @param channel
     * @param request
     * @param timeoutMillis
     * @param invokeCallback
     */
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis,
                                final InvokeCallback invokeCallback) {

    }


    class NettyEventExecuter extends ServiceThread {

        public String getServiceName() {
            return null;
        }

        public void run() {

        }
    }
}
