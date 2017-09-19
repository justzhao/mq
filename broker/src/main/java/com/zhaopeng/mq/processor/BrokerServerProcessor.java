package com.zhaopeng.mq.processor;

import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.common.client.enums.SendStatus;
import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.client.message.SendResult;
import com.zhaopeng.common.protocol.ResponseCode;
import com.zhaopeng.common.protocol.body.PullMesageInfo;
import com.zhaopeng.mq.MessageHandler;
import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.protocol.JsonSerializable;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.zhaopeng.common.protocol.RequestCode.PULL_MESSAGE;
import static com.zhaopeng.common.protocol.RequestCode.SEND_MESSAGE;

/**
 * Created by zhaopeng on 2017/5/21.
 */
public class BrokerServerProcessor implements NettyRequestProcessor {

    /**
     * 关于取消息，  会记录本次 拉取的消息 中的nextOffset (下一次 Messagequeue获取消息的偏移量) 当成下一次 Queue 的offset (队列的偏移量)，offset 信息放在 放在head。
     * <p>
     * 保存信息。
     */

    private static final Logger logger = LoggerFactory.getLogger(BrokerServerProcessor.class);

    private final MessageHandler messageHandler;

    public BrokerServerProcessor() {
        this.messageHandler = new MessageHandler();
    }


    public List<TopicInfo> getTopicInfosFromConsumeQueue(){
        return messageHandler.getTopicInfosFromConsumeQueue();
    }

    public BrokerServerProcessor(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {

        return this.processRequest(ctx.channel(), request);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand processRequest(final Channel channel, RemotingCommand request) {

        int code = request.getCode();
        switch (code) {
            case PULL_MESSAGE: {
                // 从内存中拉取数据
                return processGetMessage(channel, request);
            }
            case SEND_MESSAGE: {
                // 把消息先保存在内存中。
                processPutMessage(channel, request);
                SendResult result = new SendResult();
                result.setSendStatus(SendStatus.OK);
                RemotingCommand resp = RemotingCommand.createRequestCommand(ResponseCode.SUCCESS, null);
                resp.setBody(JsonSerializable.encode(result));
                return resp;
            }
        }
        return null;
    }

    private RemotingCommand processGetMessage(final Channel channel, RemotingCommand request) {


        RemotingCommand respone = RemotingCommand.createRequestCommand(ResponseCode.SUCCESS, null);
        byte body[] = request.getBody();
        if (body != null) {
            PullMesageInfo pullMesageInfo = PullMesageInfo.decode(body, PullMesageInfo.class);
            respone = messageHandler.getRespone(pullMesageInfo);
        }
        return respone;

    }

    private void processPutMessage(final Channel channel, RemotingCommand request) {

        String address = channel.remoteAddress().toString();
        byte[] body = request.getBody();
        SendMessage sendMessage = SendMessage.decode(body, SendMessage.class);
        // 这里换成 MessageExtBrokerInner
        sendMessage.setHost(address.replace("/", ""));
        messageHandler.addMessage(sendMessage);


    }
}
