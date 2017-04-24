package com.zhaopeng.common.client.enums;

/**
 * Created by zhaopeng on 2017/4/24.
 */
public enum ConsumeConcurrentlyStatus {
    SUCCESS(0,"消费成功"),
    LATER(1,"消费失败，待会在试一次")
    ;

    private  int value;

    private String desc;

    ConsumeConcurrentlyStatus(int value, String desc) {
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
