package com.zhaopeng.mq.producer.impl;

import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.common.client.message.SendResult;
import com.zhaopeng.mq.consumer.impl.ClientRemotingProcessor;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.mq.producer.AbstractMQProducer;
import com.zhaopeng.mq.producer.MQProducer;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClientConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaopeng on 2017/5/8.
 */
public class DefaultMQProducer extends AbstractMQProducer implements MQProducer {

    private volatile int defaultTopicQueueNums = 4;
    private int sendMsgTimeout = 3000;
    private int compressMsgBodyOverHowmuch = 1024 * 4;
    private int retryTimesWhenSendFailed = 2;
    private int retryTimesWhenSendAsyncFailed = 2;

    private int maxMessageSize = 1024 * 1024 * 4; // 4M

    private final ExecutorService executorService;



    private final MQProducerOperation mqProducerOperation;

    public DefaultMQProducer(NettyClientConfig nettyClientConfig,String addr) {
        super(nettyClientConfig);
        this.mqProducerOperation = new MQProducerOperation(nettyClientConfig,addr);

        this.executorService = Executors.newFixedThreadPool(10, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });
    }

    public void init() {
        nettyClient.start();
        nettyClient.registerDefaultProcessor(new ClientRemotingProcessor(),executorService);
    }




    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return mqProducerOperation.send(msg,sendMsgTimeout);
    }


    @Override
    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return mqProducerOperation.send(msg,timeout);
    }
}
