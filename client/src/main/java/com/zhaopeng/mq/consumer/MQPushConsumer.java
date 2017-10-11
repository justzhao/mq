package com.zhaopeng.mq.consumer;

import com.zhaopeng.mq.consumer.listener.MessageListener;

/**
 * Created by zhaopeng on 2017/10/11.
 */
public interface MQPushConsumer {


    void start() ;

    void shutdown();


    void registerMessageListener(MessageListener messageListener);

}
