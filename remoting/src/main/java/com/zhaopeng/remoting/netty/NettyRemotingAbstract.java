package com.zhaopeng.remoting.netty;

import com.zhaopeng.remoting.ChannelEventListener;
import com.zhaopeng.remoting.InvokeCallback;
import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.common.Pair;
import com.zhaopeng.remoting.common.ServiceThread;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import com.zhaopeng.remoting.protocol.RemotingSysResponseCode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by zhaopeng on 2017/3/26.
 */
public abstract class NettyRemotingAbstract {
    private static final Logger logger = LoggerFactory.getLogger(NettyRemotingAbstract.class);


    // 不需要返回值的调用的信号量个数
    protected final Semaphore semaphoreOneway;

    //异步执行任务的信号量个数
    protected final Semaphore semaphoreAsync;

    // 用来存放发出去的request
    protected final ConcurrentHashMap<Integer /* requestId */, ResponseFuture> responseTable =
            new ConcurrentHashMap<>(256);


    // 根据请求的业务类型存放的业务处理器
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable = new HashMap<>(64);

    protected final NettyEventExecuter nettyEventExecuter = new NettyEventExecuter();

    /**
     * 获取监听器
     *
     * @return
     */
    public abstract ChannelEventListener getChannelEventListener();


    /**
     * 添加一个待监听的事件
     *
     * @param event
     */
    public void putNettyEvent(NettyEvent event) {
        this.nettyEventExecuter.putNettyEvent(event);
    }


    //默认的业务处理器
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }


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
        final int requestId = cmd.getOpaque();
        if (pair != null) {
            Runnable run = new Runnable() {
                public void run() {
                    try {
                        RemotingCommand response = pair.getObject1().processRequest(ctx, cmd);
                        if (!cmd.isOneWay()) {
                            response.setOpaque(requestId);
                            response.markResponseType();
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
            response.setOpaque(requestId);
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
        int requestId = cmd.getOpaque();
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


    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<>();
        Iterator<Map.Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();
            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {

                it.remove();
                rfList.add(rep);
                logger.warn("remove timeout request, " + rep);
            }
        }

        for (ResponseFuture rf : rfList) {
            try {
                rf.executeInvokeCallback();
            } catch (Throwable e) {
                logger.warn("scanResponseTable, operationComplete Exception", e);
            }
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

        final int requestId = request.getOpaque();

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
                                final InvokeCallback invokeCallback) throws InterruptedException, RemotingException {

        final int requestId = request.getOpaque();
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {

            final ResponseFuture responseFuture = new ResponseFuture(requestId, timeoutMillis, invokeCallback);
            this.responseTable.put(requestId, responseFuture);
            try {
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        if (f.isSuccess()) {
                            responseFuture.setSendRequestOK(true);
                            return;
                        } else {
                            responseFuture.setSendRequestOK(false);
                        }

                        responseFuture.putResponse(null);
                        responseTable.remove(requestId);
                        try {
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            logger.warn("excute callback in writeAndFlush addListener, and callback throw", e);
                        } finally {
                            semaphoreAsync.release();
                        }

                        logger.warn("send a request command to channel <{}> failed.", channel);
                    }
                });
            } catch (Exception e) {
                this.semaphoreAsync.release();
                logger.warn("send a request command to channel <{}> Exception {}", channel, e);
                throw new RemotingException(channel.toString(), e);
            }
        } else {
            String info =
                    String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
                            timeoutMillis, //
                            this.semaphoreAsync.getQueueLength(), //
                            this.semaphoreAsync.availablePermits()//
                    );
            logger.warn(info);
            throw new RemotingException(info);
        }

    }

    /**
     * 不需要返回值的请求
     *
     * @param channel
     * @param request
     * @param timeoutMillis
     */
    public void invokeOneWayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingException {

        request.markOnewayRPC();
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {

            try {
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {

                        NettyRemotingAbstract.this.semaphoreOneway.release();
                        if (!f.isSuccess()) {
                            logger.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                        }
                    }
                });
            } catch (Exception e) {
                NettyRemotingAbstract.this.semaphoreOneway.release();
                ;
                logger.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingException(channel.toString(), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingException("invokeOnewayImpl invoke too fast");
            } else {
                String info = String.format(
                        "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", //
                        timeoutMillis, //
                        this.semaphoreOneway.getQueueLength(), //
                        this.semaphoreOneway.availablePermits()//
                );
                logger.warn(info);
                throw new RemotingException(info);
            }
        }

    }

    class NettyEventExecuter extends ServiceThread {
        private LinkedBlockingQueue<NettyEvent> events = new LinkedBlockingQueue<>();

        private final int maxSize = 100;


        public void putNettyEvent(final NettyEvent event) {
            if (this.events.size() <= maxSize) {
                this.events.add(event);
            } else {
                logger.warn("event queue size[{}] enough, so drop this event {}", this.events.size(), event.toString());
            }
        }

        public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        public void run() {
            logger.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStoped()) {
                try {
                    NettyEvent event = this.events.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    logger.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            logger.info(this.getServiceName() + " service end");

        }
    }
}
