
package com.zhaopeng.common.client.message;

import com.alibaba.fastjson.annotation.JSONField;

public class SubscriptionData implements Comparable<SubscriptionData> {
    public final static String SUB_ALL = "*";
    private boolean classFilterMode = false;
    private String topic;
    private String subString;

    private long subVersion = System.currentTimeMillis();
    private String expressionType;

    @JSONField(serialize = false)
    private String filterClassSource;

    public SubscriptionData() {

    }

    public SubscriptionData(String topic, String subString) {
        super();
        this.topic = topic;
        this.subString = subString;
    }

    public String getFilterClassSource() {
        return filterClassSource;
    }

    public void setFilterClassSource(String filterClassSource) {
        this.filterClassSource = filterClassSource;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSubString() {
        return subString;
    }

    public void setSubString(String subString) {
        this.subString = subString;
    }



    public long getSubVersion() {
        return subVersion;
    }

    public void setSubVersion(long subVersion) {
        this.subVersion = subVersion;
    }



    public boolean isClassFilterMode() {
        return classFilterMode;
    }

    public void setClassFilterMode(boolean classFilterMode) {
        this.classFilterMode = classFilterMode;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(String expressionType) {
        this.expressionType = expressionType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (classFilterMode ? 1231 : 1237);

        result = prime * result + ((subString == null) ? 0 : subString.hashCode());

        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + ((expressionType == null) ? 0 : expressionType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SubscriptionData other = (SubscriptionData) obj;
        if (classFilterMode != other.classFilterMode)
            return false;

        if (subString == null) {
            if (other.subString != null)
                return false;
        } else if (!subString.equals(other.subString))
            return false;
        if (subVersion != other.subVersion)
            return false;

        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        if (expressionType == null) {
            if (other.expressionType != null)
                return false;
        } else if (!expressionType.equals(other.expressionType))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SubscriptionData{" +
                "classFilterMode=" + classFilterMode +
                ", topic='" + topic + '\'' +
                ", subString='" + subString + '\'' +
                ", subVersion=" + subVersion +
                ", expressionType='" + expressionType + '\'' +
                ", filterClassSource='" + filterClassSource + '\'' +
                '}';
    }

    @Override
    public int compareTo(SubscriptionData other) {
        String thisValue = this.topic + "@" + this.subString;
        String otherValue = other.topic + "@" + other.subString;
        return thisValue.compareTo(otherValue);
    }
}
