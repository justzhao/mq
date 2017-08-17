package com.zhaopeng.mq;

import com.google.common.collect.Maps;
import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.protocol.ResponseCode;
import com.zhaopeng.common.protocol.body.PullMesageInfo;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import com.zhaopeng.store.jvm.JvmMessageStore;
import com.zhaopeng.store.MessageStore;
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

    public void addMessage(SendMessage sendMessage) {
        String topic = sendMessage.getTopic();

        MessageStore store = topicStore.get(topic);
        if (store == null) {
            store = new JvmMessageStore();
            topicStore.put(topic, store);
        }
        store.addMessage(sendMessage);

    }


    public Message getMessage(PullMesageInfo pull) {
        int queueId = pull.getQueueId();
        MessageStore store = topicStore.get(pull.getTopic());
        if (store == null) {
            return null;
        }
        return store.getMessage(pull);
    }


    public RemotingCommand getRespone(){

        RemotingCommand respone = RemotingCommand.createRequestCommand(ResponseCode.SUCCESS, null);



        return respone;
    }


}
