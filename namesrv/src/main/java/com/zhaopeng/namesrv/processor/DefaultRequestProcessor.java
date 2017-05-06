package com.zhaopeng.namesrv.processor;

import com.zhaopeng.common.protocol.RequestCode;
import com.zhaopeng.common.protocol.body.RegisterBrokerInfo;
import com.zhaopeng.common.protocol.body.RegisterBrokerResult;
import com.zhaopeng.common.protocol.body.TopicList;
import com.zhaopeng.common.protocol.route.TopicRouteData;
import com.zhaopeng.namesrv.NameSrvController;
import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.exception.RemotingException;
import com.zhaopeng.remoting.protocol.JsonSerializable;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import com.zhaopeng.remoting.protocol.RemotingSysResponseCode;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;


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

    /**
     * 根据body 里面的信息把broker注册到routerManager中去
     * 返回注册结果信息
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingException
     */
    public RemotingCommand registerBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingException {

        RemotingCommand respone = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, null);
        RegisterBrokerInfo registerBrokerInfo = null;
        if (request.getBody() != null) {
            registerBrokerInfo = RegisterBrokerInfo.decode(request.getBody(), RegisterBrokerInfo.class);
        } else {
            registerBrokerInfo = new RegisterBrokerInfo();
            registerBrokerInfo.getDataVersion().setCounter(new AtomicLong(0));
            registerBrokerInfo.getDataVersion().setTimestatmp(0);
        }

        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(registerBrokerInfo, ctx.channel());
        respone.setBody(result.encode());
        return respone;
    }

    /**
     * 根据body 从删除routerManager删除broker
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingException
     */
    public RemotingCommand unregisterBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingException {

        RemotingCommand respone = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, null);
        RegisterBrokerInfo registerBrokerInfo = null;
        if (request.getBody() != null) {
            registerBrokerInfo = RegisterBrokerInfo.decode(request.getBody(), RegisterBrokerInfo.class);
            this.namesrvController.getRouteInfoManager().unRegisterBroker(registerBrokerInfo);
        }

        return respone;
    }

    /**
     * 根据主题信息获取broker路由
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingException
     */
    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingException {

        RemotingCommand respone = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, null);

        if (request.getBody() != null) {
            String topic = new String(request.getBody());
            TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().selectTopicRouteData(topic);
            if (topicRouteData != null) {
                respone.setBody(topicRouteData.encode());
            }
        }

        return respone;
    }

    /**
     * 获取broker集群信息
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingException
     */
    public RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingException {

        return null;
    }


    /**
     * 删除topic信息
     *
     * @param ctx
     * @param request
     * @return
     * @throws RemotingException
     */
    private RemotingCommand deleteTopicInNamesrv(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingException {

        RemotingCommand respone = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, null);

        String topic = null;
        if (request.getBody() != null) {
            topic = JsonSerializable.decode(request.getBody(), String.class);
            this.namesrvController.getRouteInfoManager().deleteTopic(topic);
        }
        return respone;

    }

    public RemotingCommand getAllTopicListFromNameserver(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingException {

        RemotingCommand respone = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, null);

        TopicList topicList = this.namesrvController.getRouteInfoManager().getAllTopicList();
        if (topicList != null) {
            respone.setBody(topicList.encode());
        }
        return respone;
    }

}
