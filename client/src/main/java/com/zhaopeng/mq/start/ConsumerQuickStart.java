package com.zhaopeng.mq.start;

import com.zhaopeng.mq.consumer.impl.DefaultMQPullConsumer;
import com.zhaopeng.remoting.netty.NettyClientConfig;

/**
 * Created by zhaopeng on 2017/7/10.
 */
public class ConsumerQuickStart {


    public static void main(String args[]){

        NettyClientConfig clientConfig=new NettyClientConfig();
        String addr="127.0.0.1";
        DefaultMQPullConsumer consumer=new DefaultMQPullConsumer(clientConfig,addr);
        consumer.init();

    }
}
