package com.zhaopeng.common.protocol;

/**
 * Created by zhaopeng on 2017/4/11.
 */
public class RequestCode {

    public static final int HEART_BEAT = 1000;
    public static final int REGISTER_BROKER = 1001;
    public static final int UNREGISTER_BROKER = 1002;
    public static final int GET_ROUTEINTO_BY_TOPIC = 1003;
    public static final int GET_BROKER_CLUSTER_INFO = 1004;
    public static final int GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 1005;
    public static final int DELETE_TOPIC_IN_NAMESRV = 1006;


    public static final int NOTIFY_CONSUMER_IDS_CHANGED = 1007;
    public static final int RESET_CONSUMER_CLIENT_OFFSET = 1008;
    public static final int GET_CONSUMER_STATUS_FROM_CLIENT = 1009;
    public static final int GET_CONSUMER_RUNNING_INFO = 1010;
    public static final int CONSUME_MESSAGE_DIRECTLY = 1011;

    public static final int SEARCH_OFFSET_BY_TIMESTAMP=1012;

    public static final int CREATE_TOPIC=1013;


}
