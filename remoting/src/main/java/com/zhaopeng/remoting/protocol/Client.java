package com.zhaopeng.remoting.protocol;

import com.zhaopeng.remoting.InvokeCallback;
import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.exception.RemotingException;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Created by zhaopeng on 2017/4/26.
 */
public interface Client {

    void start();


    void shutdown();

    public void updateNameServerAddressList(final List<String> addrs);


    public List<String> getNameServerAddressList();


    public RemotingCommand invokeSync(final String addr, final RemotingCommand request,
                                      final long timeoutMillis) throws InterruptedException, RemotingException;


    public void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
                            final InvokeCallback invokeCallback) throws InterruptedException, RemotingException;


    public void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingException;


    public void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
                                  final ExecutorService executor);


    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);



    public boolean isChannelWriteable(final String addr);
}
