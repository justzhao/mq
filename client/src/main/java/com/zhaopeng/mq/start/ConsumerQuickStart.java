package com.zhaopeng.mq.start;

import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.common.client.message.PullResult;
import com.zhaopeng.mq.consumer.impl.DefaultMQPullConsumer;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Created by zhaopeng on 2017/7/10.
 */
public class ConsumerQuickStart {


    private static final Logger log = LoggerFactory.getLogger(ConsumerQuickStart.class);


    public static void main(String args[]) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        NettyClientConfig clientConfig = new NettyClientConfig();
        // namsrv的 地址
        String addr = "127.0.0.1:9876";
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(clientConfig, addr);
        consumer.init();

        String topic = "zhaopeng";

        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);

        while (true) {
            for (MessageQueue q : mqs) {
                PullResult result = consumer.pull(q, "", 0, 10);
                log.info("the result is {}", result.getMessages());
            }
        }

    }
}
