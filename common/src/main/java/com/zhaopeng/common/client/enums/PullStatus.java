package com.zhaopeng.common.client.enums;

/**
 * Created by zhaopeng on 2017/4/25.
 */
public enum PullStatus {

    FOUND(1,"发现了消息"),

    NO_NEW_MSG(2,"没有消息"),

    NO_MATCHED_MSG(3,"没有匹配的消息"),

    OFFSET_ILLEGAL(4,"非法的偏移量"),
    FAIL(5,"失败了"),
    ;


    private  int value;

    private String desc;

    PullStatus(int value, String desc) {
        this.value = value;
        this.desc = desc;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
