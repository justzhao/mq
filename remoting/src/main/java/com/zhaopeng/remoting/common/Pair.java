package com.zhaopeng.remoting.common;

/**
 * Created by zhaopeng on 2017/3/31.
 */
public class Pair<T1, T2> {

    private T1 object1;
    private T2 object2;

    public T1 getObject1() {
        return object1;
    }

    public void setObject1(T1 object1) {
        this.object1 = object1;
    }

    public T2 getObject2() {
        return object2;
    }

    public void setObject2(T2 object2) {
        this.object2 = object2;
    }
}
