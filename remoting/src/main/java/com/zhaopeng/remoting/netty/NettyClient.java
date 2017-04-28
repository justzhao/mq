package com.zhaopeng.remoting.netty;

import com.zhaopeng.remoting.ChannelEventListener;
import com.zhaopeng.remoting.InvokeCallback;
import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.common.RemotingHelper;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.protocol.Client;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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
                                new NettyConnetManageHandler(), //
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

    }

    @Override
    public void updateNameServerAddressList(List<String> addrs) {

    }

    @Override
    public List<String> getNameServerAddressList() {

        return null;
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

    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingException {

        Channel channel = getAndCreateChannel(addr);
        if (channel == null) {
            return ;
        }
        this.invokeOneWayImpl(channel,request,timeoutMillis);
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {

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


        final Channel channel = channelTables.get(addr);


        if (channel == null) {

            try {
                this.lockChannelTables.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS);


                ChannelFuture future = this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
                logger.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                future.addListener(new GenericFutureListener<Future<? super Void>>() {
                    @Override
                    public void operationComplete(Future<? super Void> future) throws Exception {

                        if (future.isSuccess()) {
                            channelTables.put(addr, channel);
                        }
                    }
                });


            } catch (InterruptedException e) {

                logger.info("thread {} is Interrupted {}", Thread.currentThread().getName(), e);

            } finally {
                this.lockChannelTables.unlock();
            }

        }

        return channel;

    }


    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);

        }
    }

    class NettyConnetManageHandler extends ChannelDuplexHandler {

    }


}