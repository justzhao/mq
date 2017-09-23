package com.zhaopeng.store;


import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.protocol.body.PullMesageInfo;
import com.zhaopeng.store.disk.GetMessageResult;
import com.zhaopeng.store.entity.PutMessageResult;

import java.util.List;

/**
 * Created by zhaopeng on 2017/7/24.
 */
public interface MessageStore {


    /**
     * 直获取响应
     * @param pull
     * @return
     */
    public GetMessageResult getMessageContent(PullMesageInfo pull);

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

    /**
     * 初始化加载
     */
    public void load();


    /**
     * 从
     * @return
     */
    public List<TopicInfo> getTopicInfosFromConsumeQueue();
}
