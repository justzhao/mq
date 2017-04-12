package com.zhaopeng.common.protocol.body;

import com.zhaopeng.common.DataVersion;
import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.remoting.protocol.JsonSerializable;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaopeng on 2017/4/12.
 */
public class RegisterBrokerInfo extends JsonSerializable {

    private DataVersion dataVersion= new DataVersion();

    private ConcurrentHashMap<String, TopicInfo> topicConfigTable =
            new ConcurrentHashMap<>();

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public ConcurrentHashMap<String, TopicInfo> getTopicConfigTable() {
        return topicConfigTable;
    }

    public void setTopicConfigTable(ConcurrentHashMap<String, TopicInfo> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }
}
