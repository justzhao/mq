package com.zhaopeng.store.disk;

import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.protocol.body.PullMesageInfo;
import com.zhaopeng.store.MessageStore;

/**
 * Created by zhaopeng on 2017/7/27.
 */
public class DiskMessageStore implements MessageStore {
    @Override
    public Message getMessage(PullMesageInfo pull) {
        return null;
    }

    @Override
    public void addMessage(SendMessage sendMessage) {

    }
}
