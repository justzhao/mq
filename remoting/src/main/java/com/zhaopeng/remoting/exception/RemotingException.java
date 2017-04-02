package com.zhaopeng.remoting.exception;

/**
 * Created by zhaopeng on 2017/4/2.
 */
public class RemotingException extends Exception {

    public RemotingException(String message) {
        super(message);
    }


    public RemotingException(String message, Throwable cause) {
        super(message, cause);
    }

}
