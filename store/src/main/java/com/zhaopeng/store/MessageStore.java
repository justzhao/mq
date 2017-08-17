package com.zhaopeng.store;


import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.protocol.body.PullMesageInfo;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import com.zhaopeng.store.entity.PutMessageResult;

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
     * 直获取响应
     * @param pull
     * @return
     */
    public RemotingCommand getMessageContent(PullMesageInfo pull);

    /**
     * 添加消息
     * @param sendMessage
     */
    public PutMessageResult addMessage(SendMessage sendMessage);


    /**
     * 启动服务
     */
    public void start();

    /**
     * 关闭服务
     */
    public void shutDown();
}
