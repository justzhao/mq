package com.zhaopeng.namesrv.processor;

import com.zhaopeng.common.protocol.RequestCode;
import com.zhaopeng.namesrv.NameSrvController;
import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by zhaopeng on 2017/4/5.
 */
public class DefaultRequestProcessor implements NettyRequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultRequestProcessor.class);


    protected final NameSrvController namesrvController;


    public DefaultRequestProcessor(NameSrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    /**
     * 根据request 中的业务操作码来实现 不同的业务类型
     *
     * @param ctx
     * @param request
     * @return
     * @throws Exception
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        logger.info("receive request {}  {}", ctx, request);


        switch (request.getCode()) {

            case RequestCode.REGISTER_BROKER:

                return this.registerBroker(ctx, request);

            case RequestCode.UNREGISTER_BROKER:
                return this.unregisterBroker(ctx, request);
            case RequestCode.GET_ROUTEINTO_BY_TOPIC:
                return this.getRouteInfoByTopic(ctx, request);

            case RequestCode.GET_BROKER_CLUSTER_INFO:
                return this.getBrokerClusterInfo(ctx, request);

            case RequestCode.DELETE_TOPIC_IN_NAMESRV:
                return this.deleteTopicInNamesrv(ctx, request);

            case RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
                return getAllTopicListFromNameserver(ctx, request);
            default:
                break;

        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand registerBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingException {


        return null;
    }

    public RemotingCommand unregisterBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingException {

        return null;
    }

    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingException {

        return null;
    }

    public RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingException {

        return null;
    }


    private RemotingCommand deleteTopicInNamesrv(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingException {


        return null;

    }

    public RemotingCommand getAllTopicListFromNameserver(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingException {

        return null;
    }

}
