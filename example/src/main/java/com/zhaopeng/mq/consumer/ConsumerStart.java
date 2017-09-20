package com.zhaopeng.mq.consumer;

import com.zhaopeng.common.client.message.MessageQueue;
import com.zhaopeng.common.client.message.PullResult;
import com.zhaopeng.common.message.PullMessage;
import com.zhaopeng.mq.consumer.impl.DefaultMQPullConsumer;
import com.zhaopeng.mq.exception.MQBrokerException;
import com.zhaopeng.mq.exception.MQClientException;
import com.zhaopeng.mq.start.ConsumerQuickStart;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.netty.NettyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhaopeng on 2017/5/7.
 */
public class ConsumerStart {

    private static final Logger log = LoggerFactory.getLogger(ConsumerQuickStart.class);

    private static final Map<MessageQueue, Long> offseTable = new HashMap<>();

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
                PullResult result = consumer.pull(q, "", getMessageQueueOffset(q), 1);
                putMessageQueueOffset(q, result.getNextBeginOffset());
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


    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = offseTable.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        offseTable.put(mq, offset);
    }

}
