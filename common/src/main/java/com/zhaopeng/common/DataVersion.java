package com.zhaopeng.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhaopeng on 2017/4/12.
 * 关于通信数据的版本
 */


public class DataVersion {
    private long timestatmp = System.currentTimeMillis();
    private AtomicLong counter = new AtomicLong(0);

    public long getTimestatmp() {
        return timestatmp;
    }

    public void setTimestatmp(long timestatmp) {
        this.timestatmp = timestatmp;
    }

    public AtomicLong getCounter() {
        return counter;
    }

    public void setCounter(AtomicLong counter) {
        this.counter = counter;
    }

    @Override
    public String toString() {
        return "DataVersion{" +
                "timestatmp=" + timestatmp +
                ", counter=" + counter +
                '}';
    }
}
