package com.zhaopeng.mq;

import com.google.common.collect.Maps;
import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.mq.store.MessageStore;

import java.util.Map;

/**
 * Created by zhaopeng on 2017/6/28.
 */
public class MessageHandler {

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
        store.getQueue().add(message);

    }


}
