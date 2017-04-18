package com.zhaopeng.common.protocol.body;

import com.zhaopeng.remoting.protocol.JsonSerializable;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhaopeng on 2017/4/18.
 */
public class TopicList extends JsonSerializable implements Serializable {
    private static final long serialVersionUID = -3467397682548665336L;

    private Set<String> topicList = new HashSet<String>();


    public Set<String> getTopicList() {
        return topicList;
    }

    public void setTopicList(Set<String> topicList) {
        this.topicList = topicList;
    }


}
