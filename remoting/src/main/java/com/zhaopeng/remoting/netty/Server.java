package com.zhaopeng.remoting.netty;

import com.zhaopeng.remoting.common.Pair;

import java.util.concurrent.ExecutorService;

/**
 * Created by zhaopeng on 2017/3/23.
 */
public interface Server {

    void start();


    void shutdown();

    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
                           final ExecutorService executor);

    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);


    /**
     * 根据业务操作码获取对应的处理程序
     * @param requestCode
     * @return
     */
    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);
}
