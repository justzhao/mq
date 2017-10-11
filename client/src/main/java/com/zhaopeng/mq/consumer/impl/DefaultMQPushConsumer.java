package com.zhaopeng.mq.consumer.impl;

import com.zhaopeng.mq.consumer.MQPushConsumer;
import com.zhaopeng.mq.consumer.listener.MessageListener;

import java.util.concurrent.ExecutorService;

/**
 * Created by zhaopeng on 2017/10/11.
 */
public class DefaultMQPushConsumer implements MQPushConsumer {

    private final ExecutorService executorService;
    // namesrv的地址
    private String addr;

    public DefaultMQPushConsumer(ExecutorService executorService) {
        this.executorService = executorService;
    }


    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void registerMessageListener(MessageListener messageListener) {

    }
}
