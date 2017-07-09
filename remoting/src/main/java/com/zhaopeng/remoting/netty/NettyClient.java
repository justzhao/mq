package com.zhaopeng.remoting.netty;

import com.zhaopeng.remoting.ChannelEventListener;
import com.zhaopeng.remoting.InvokeCallback;
import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.common.Pair;
import com.zhaopeng.remoting.common.RemotingHelper;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.protocol.Client;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zhaopeng on 2017/4/26.
 */
public class NettyClient extends NettyRemotingAbstract implements Client {

    private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);

    private final NettyClientConfig nettyClientConfig;

    private final ChannelEventListener channelEventListener;

    private Bootstrap bootstrap;

    private final ExecutorService publicExecutor;

    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    private final EventLoopGroup eventLoopGroupWorker;

    private final Timer timer = new Timer("ClientHouseKeepingService", true);

    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<>();

    private final ConcurrentHashMap<String /* addr */, Channel> channelTables = new ConcurrentHashMap<>();
    private static final long LockTimeoutMillis = 3000;
    private final Lock lockChannelTables = new ReentrantLock();

    public NettyClient(NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public NettyClient(final NettyClientConfig nettyClientConfig, //
                       final ChannelEventListener channelEventListener) {
        super(10, 10);
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        bootstrap = new Bootstrap();

        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });

        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);


            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
            }
        });

    }


    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(//
                nettyClientConfig.getClientWorkerThreads(), //
                new ThreadFactory() {

                    private AtomicInteger threadIndex = new AtomicInteger(0);


                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                    }
                });

        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)//
                //
                .option(ChannelOption.TCP_NODELAY, true)
                //
                .option(ChannelOption.SO_KEEPALIVE, false)
                //
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
                //
                .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getSocketSndbufSize())
                //
                .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getSocketRcvbufSize())
                //
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(//
                                defaultEventExecutorGroup, //
                                new NettyEncoder(), //
                                new NettyDecoder(), //
                                new IdleStateHandler(0, 0, 100), //
                                new NettyConnectManageHandler(), //
                                new NettyClientHandler());
                    }
                });

        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                NettyClient.this.scanResponseTable();

            }
        }, 1000 * 3, 1000);

        if (this.channelEventListener != null) {
            this.nettyEventExecuter.start();
        }
    }

    @Override
    public void shutdown() {
        defaultEventExecutorGroup.shutdownGracefully();
        eventLoopGroupWorker.shutdownGracefully();
        timer.cancel();
    }

    @Override
    public void updateNameServerAddressList(List<String> addrs) {

        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if (!addrs.isEmpty()) {
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (int i = 0; i < addrs.size() && !update; i++) {
                    if (!old.contains(addrs.get(i))) {
                        update = true;
                    }
                }
            }

            if (update) {
                Collections.shuffle(addrs);
                this.namesrvAddrList.set(addrs);
            }
        }

    }

    @Override
    public List<String> getNameServerAddressList() {

        return this.namesrvAddrList.get();
    }

    @Override
    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingException {


        Channel channel = getAndCreateChannel(addr);
        if (channel == null) {
            return null;
        }
        RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis);

        return response;
    }

    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws InterruptedException, RemotingException {

        Channel channel = getAndCreateChannel(addr);
        if (channel == null) {
            return;
        }

        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);

    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingException {

        Channel channel = getAndCreateChannel(addr);
        if (channel == null) {
            return;
        }
        this.invokeOneWayImpl(channel, request, timeoutMillis);
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {


        if (null == executor) {
            executor = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executor);
        this.processorTable.put(requestCode, pair);

    }

    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {

        if (null == executor) {
            executor = this.publicExecutor;
        }
        this.defaultRequestProcessor = new Pair<>(processor, executor);
    }

    @Override
    public boolean isChannelWriteable(String addr) {
        return false;
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return null;
    }

    private Channel getAndCreateChannel(final String addr) {
        Channel channel = channelTables.get(addr);
        if (channel == null) {
            try {
                this.lockChannelTables.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS);
                final ChannelFuture f = this.bootstrap.connect(addr, 9876);
                logger.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                if (f.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                    if (ischannelFutureOK(f)) {
                        logger.info("createChannel: connect remote host[{}] success, {}", addr, f.toString());
                        return f.channel();
                    } else {
                        logger.info("createChannel: connect remote host[" + addr + "] failed, " + f.toString(), f.cause());
                    }
                }
            } catch (InterruptedException e) {

                logger.info("thread {} is Interrupted {}", Thread.currentThread().getName(), e);

            } catch (Exception e) {
                logger.info("getChannelEventListener error {}", e);
            } finally {
                this.lockChannelTables.unlock();
            }

        }

        return null;

    }


    public boolean ischannelFutureOK(ChannelFuture channelFuture) {
        return channelFuture.channel() != null && channelFuture.channel().isActive();
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);

        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise)
                throws Exception {
            final String local = localAddress == null ? "UNKNOW" : localAddress.toString();
            final String remote = remoteAddress == null ? "UNKNOW" : remoteAddress.toString();
            logger.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);
            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyClient.this.channelEventListener != null) {
                NettyClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress.toString(), ctx.channel()));
            }
        }


        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            logger.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            ctx.channel().close();
            super.disconnect(ctx, promise);

            if (NettyClient.this.channelEventListener != null) {
                NettyClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress.toString(), ctx.channel()));
            }
        }


        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            logger.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            ctx.channel().close();
            super.close(ctx, promise);

            if (NettyClient.this.channelEventListener != null) {
                NettyClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress.toString(), ctx.channel()));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent evnet = (IdleStateEvent) evt;
                if (evnet.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    logger.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", evt);
                    ctx.channel().close();
                    if (NettyClient.this.channelEventListener != null) {
                        NettyClient.this
                                .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            logger.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            logger.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            ctx.channel().close();
            if (NettyClient.this.channelEventListener != null) {
                NettyClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }
        }

    }


}