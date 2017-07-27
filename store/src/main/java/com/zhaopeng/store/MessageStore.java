package com.zhaopeng.store;


import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.protocol.body.PullMesageInfo;
import com.zhaopeng.common.client.message.Message;

/**
 * Created by zhaopeng on 2017/7/24.
 */
public interface MessageStore {

    /**
     * 获取消息
     * @return
     */
    public Message getMessage(PullMesageInfo pull);

    /**
     * 添加消息
     * @param sendMessage
     */
    public void  addMessage(SendMessage sendMessage);
}
