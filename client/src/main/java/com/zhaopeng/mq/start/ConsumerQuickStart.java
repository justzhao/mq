package com.zhaopeng.mq.start;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhaopeng on 2017/7/10.
 */
public class ConsumerQuickStart {


    private static final Logger log = LoggerFactory.getLogger(ConsumerQuickStart.class);

/*
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
                PullResult result = consumer.pull(q, "", 0, 1);
                List<PullMessage> msgs = result.getMsgs();
                if (msgs == null) {
                    log.info("no message");
                } else {
                    for (PullMessage m : msgs) {
                        log.info("msg is {}, content is {}", m, new String(m.getBody()));
                    }
                }

            }
        }

    }*/
}
