package com.zhaopeng.common.client.message;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by zhaopeng on 2017/4/23.
 */
public class Message implements Serializable {


    private static final long serialVersionUID = 1879985075092970578L;
    private String topic;
    private int flag;
    private Map<String, String> properties;
    private byte[] body;


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
