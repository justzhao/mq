package com.zhaopeng.mq.processor;

import com.zhaopeng.common.client.enums.SendStatus;
import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.client.message.SendResult;
import com.zhaopeng.common.protocol.ResponseCode;
import com.zhaopeng.mq.MessageHandler;
import com.zhaopeng.remoting.NettyRequestProcessor;
import com.zhaopeng.remoting.protocol.JsonSerializable;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

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

            }
            case SEND_MESSAGE: {
                //这里要使用线程池操作。
                processMessage(request);

                SendResult result = new SendResult();

                result.setSendStatus(SendStatus.OK);


                RemotingCommand resp = RemotingCommand.createRequestCommand(ResponseCode.SUCCESS, null);

                resp.setBody(JsonSerializable.encode(result));

                return resp;


            }
        }
        return null;
    }


    private void processMessage(RemotingCommand request) {


        byte[] body = request.getBody();

        SendMessage sendMessage = SendMessage.decode(body, SendMessage.class);

        messageHandler.addMessage(sendMessage.getTopic(),sendMessage.getMsg());


    }
}
