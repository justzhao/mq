package com.zhaopeng.common.protocol.body;

import com.zhaopeng.common.DataVersion;
import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.remoting.protocol.JsonSerializable;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaopeng on 2017/4/12.
 */
public class RegisterBrokerInfo extends JsonSerializable  implements Serializable{


    private static final long serialVersionUID = -4425143580279939193L;
    private Long brokerId;

    private String brokerName;

    private String serverAddr;


    private DataVersion dataVersion = new DataVersion();

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

    public String getServerAddr() {
        return serverAddr;
    }

    public void setServerAddr(String serverAddr) {
        this.serverAddr = serverAddr;
    }


    public Long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(Long brokerId) {
        this.brokerId = brokerId;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
