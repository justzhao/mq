package com.zhaopeng.remoting.netty;

/**
 * Created by zhaopeng on 2017/4/3.
 * 定义netty中包含的四种事件
 */
public enum  NettyEventType {
    CONNECT,
    CLOSE,
    IDLE,
    EXCEPTION
}
