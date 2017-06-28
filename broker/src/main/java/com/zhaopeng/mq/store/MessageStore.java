package com.zhaopeng.mq.store;

import com.zhaopeng.common.client.message.Message;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by zhaopeng on 2017/6/28.
 */
public class MessageStore {

    Queue<Message> queue = new LinkedList<>();


    public Queue<Message> getQueue() {
        return queue;
    }

    public void setQueue(Queue<Message> queue) {
        this.queue = queue;
    }
}
