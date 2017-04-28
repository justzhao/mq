package com.zhaopeng.remoting.common;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Created by zhaopeng on 2017/4/28.
 */
public class RemotingHelper {

    public static SocketAddress string2SocketAddress(final String addr) {
        String[] s = addr.split(":");
        InetSocketAddress socketAddress = new InetSocketAddress(s[0], Integer.parseInt(s[1]));
        return socketAddress;
    }
}
