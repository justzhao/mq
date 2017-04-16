package com.zhaopeng.common.protocol.route;

import com.zhaopeng.remoting.protocol.JsonSerializable;

import java.util.List;

/**
 * Created by zhaopeng on 2017/4/16.
 * 路由信息
 */
public class TopicRouteData extends JsonSerializable {

    private String orderTopicConf;
    private List<QueueData> queueDatas;
    private List<BrokerData> brokerDatas;

    public TopicRouteData cloneOne() {
        TopicRouteData t = new TopicRouteData();
        t.setOrderTopicConf(this.getOrderTopicConf());
        t.setBrokerDatas(this.getBrokerDatas());
        t.setQueueDatas(this.getQueueDatas());
        return t;
    }


    public String getOrderTopicConf() {
        return orderTopicConf;
    }

    public void setOrderTopicConf(String orderTopicConf) {
        this.orderTopicConf = orderTopicConf;
    }

    public List<QueueData> getQueueDatas() {
        return queueDatas;
    }

    public void setQueueDatas(List<QueueData> queueDatas) {
        this.queueDatas = queueDatas;
    }

    public List<BrokerData> getBrokerDatas() {
        return brokerDatas;
    }

    public void setBrokerDatas(List<BrokerData> brokerDatas) {
        this.brokerDatas = brokerDatas;
    }
}
