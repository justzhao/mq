package com.zhaopeng.common.client.query;

import com.zhaopeng.common.client.message.MessageInfo;

import java.util.List;

/**
 * Created by zhaopeng on 2017/4/23.
 */
public class QueryResult {

    private final long indexLastUpdateTimestamp;
    private final List<MessageInfo> messageList;

    public QueryResult(long indexLastUpdateTimestamp, List<MessageInfo> messageList) {
        this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
        this.messageList = messageList;
    }

    public long getIndexLastUpdateTimestamp() {
        return indexLastUpdateTimestamp;
    }

    public List<MessageInfo> getMessageList() {
        return messageList;
    }
}
