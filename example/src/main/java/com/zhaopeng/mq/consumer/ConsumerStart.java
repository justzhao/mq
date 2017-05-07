package com.zhaopeng.mq.consumer;

import com.zhaopeng.mq.consumer.impl.DefaultMQPullConsumer;
import com.zhaopeng.remoting.netty.NettyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhaopeng on 2017/5/7.
 */
public class ConsumerStart {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerStart.class);

    public static void main(String args[]) {
        try{
            NettyClientConfig clientConfig=new NettyClientConfig();
            String namesrv="127.0.0.1:9876";
            DefaultMQPullConsumer consumer=new DefaultMQPullConsumer(clientConfig,namesrv);

            logger.info("the fetch result is {}",consumer.fetchSubscribeMessageQueues("order_system"));

        }catch (Exception e){
            e.printStackTrace();
        }



    }
}
