package com.zhaopeng.mq.producer;

import java.util.Random;

/**
 * Created by zhaopeng on 2017/5/11.
 */
public class ThreadLocalIndex {

    private final ThreadLocal<Integer> threadLocalIndex = new ThreadLocal<Integer>();
    private final Random random = new Random();
    public ThreadLocalIndex(int value) {

    }

    public int getAndIncrement() {
        Integer index = this.threadLocalIndex.get();
        if (null == index) {
            index = Math.abs(random.nextInt());
            if (index < 0) index = 0;
            this.threadLocalIndex.set(index);
        }

        index = Math.abs(index + 1);
        if (index < 0)
            index = 0;

        this.threadLocalIndex.set(index);
        return index;
    }
}
