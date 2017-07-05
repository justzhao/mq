package com.zhaopeng.mq.processor;

import com.google.common.collect.Lists;
import com.zhaopeng.common.client.enums.PullStatus;
import com.zhaopeng.common.client.enums.SendStatus;
import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.common.client.message.PullResult;
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

import java.util.List;

import static com.zhaopeng.common.protocol.RequestCode.PULL_MESSAGE;
import static com.zhaopeng.common.protocol.RequestCode.SEND_MESSAGE;

/**
 * Created by zhaopeng on 2017/5/21.
 */
public class BrokerServerProcessor implements NettyRequestProcessor {


    private final MessageHandler messageHandler;

    public BrokerServerProcessor() {
        this.messageHandler = new MessageHandler();
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

                // 把消息先保存在内存中。
                return processGetMessage(channel ,request);

            }
            case SEND_MESSAGE: {
                processPutMessage(channel,request);

                SendResult result = new SendResult();

                result.setSendStatus(SendStatus.OK);


                RemotingCommand resp = RemotingCommand.createRequestCommand(ResponseCode.SUCCESS, null);

                resp.setBody(JsonSerializable.encode(result));

                return resp;


            }
        }
        return null;
    }

    private RemotingCommand processGetMessage(final Channel channel,RemotingCommand request) {
        RemotingCommand respone = RemotingCommand.createRequestCommand(ResponseCode.SUCCESS, null);
        byte body[] = request.getBody();
        if (body != null) {
            PullMesageInfo pullMesageInfo = PullMesageInfo.decode(body, PullMesageInfo.class);
            Message message = messageHandler.getMessageByTopic(pullMesageInfo.getTopic());
            List<Message> msg = Lists.newArrayList();
            msg.add(message);
            PullResult result = null;
            if (message != null) {
                result = new PullResult(PullStatus.FOUND);
                result.setMessages(msg);
            } else {
                result = new PullResult(PullStatus.NO_NEW_MSG);
            }
            respone.setBody(result.encode());
        }
        return respone;

    }

    private void processPutMessage(final Channel channel,RemotingCommand request) {


        byte[] body = request.getBody();
        SendMessage sendMessage = SendMessage.decode(body, SendMessage.class);
        messageHandler.addMessage(sendMessage.getTopic(), sendMessage.getMsg());


    }
}
