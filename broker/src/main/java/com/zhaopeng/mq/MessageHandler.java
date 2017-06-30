package com.zhaopeng.mq;

import com.google.common.collect.Maps;
import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.mq.store.MessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by zhaopeng on 2017/6/28.
 */
public class MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);


    /**
     * 用来存放topic对应的消息
     */
    private Map<String, MessageStore> topicStore = Maps.newConcurrentMap();


    public void addMessage(String topic, Message message) {

        MessageStore store = topicStore.get(topic);
        if (store == null) {
            store = new MessageStore();
            topicStore.put(topic, store);
        }

        logger.info("{} add new msg {}", topic, message);
        store.getQueue().add(message);

    }

    public Message getMessageByTopic(String topic) {

        MessageStore store = topicStore.get(topic);
        if (store == null) {
            return null;
        }
        Message m = store.getQueue().poll();
        logger.info("{} get   msg {}", topic, m);
        return m;
    }


}
