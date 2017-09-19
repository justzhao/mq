package com.zhaopeng.mq;


import com.zhaopeng.common.TopicInfo;
import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.protocol.ResponseCode;
import com.zhaopeng.common.protocol.body.PullMesageInfo;
import com.zhaopeng.remoting.protocol.RemotingCommand;
import com.zhaopeng.store.MessageStore;
import com.zhaopeng.store.disk.DiskMessageStore;
import com.zhaopeng.store.disk.GetMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;


/**
 * Created by zhaopeng on 2017/6/28.
 */
public class MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);


    private final MessageStore store;

    public MessageHandler() {
        store = new DiskMessageStore();
        store.load();
        store.start();
    }

    public void addMessage(SendMessage sendMessage) {

        store.addMessage(sendMessage);

    }

    public List<TopicInfo> getTopicInfosFromConsumeQueue(){
        return store.getTopicInfosFromConsumeQueue();
    }

    public RemotingCommand getRespone(PullMesageInfo pull) {

        RemotingCommand response = RemotingCommand.createRequestCommand(ResponseCode.SUCCESS, null);

        GetMessageResult result = store.getMessageContent(pull);

        if (result != null) {
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    byte[] bytes = readGetMessageResult(result);

                    response.setMinOffset(result.getMinOffset());
                    response.setMaxOffset(result.getMaxOffset());
                    response.setNextBeginOffset(result.getNextBeginOffset());
                    response.setBody(bytes);
                    break;

                }
                default: {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("store getMessage return null");
                    break;
                }
            }
            return response;

        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store getMessage return null");
        }


        return response;

    }

    private byte[] readGetMessageResult(final GetMessageResult getMessageResult) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                byteBuffer.put(bb);
            }
        } finally {
            getMessageResult.release();
        }
        return byteBuffer.array();
    }


}
