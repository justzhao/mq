package com.zhaopeng.mq;


import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.protocol.ResponseCode;
import com.zhaopeng.common.protocol.body.PullMesageInfo;
import com.zhaopeng.common.store.MessageOffsetConstant;
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




    MessageStore store=new DiskMessageStore();

    public void addMessage(SendMessage sendMessage) {
     /*   String topic = sendMessage.getTopic();

        MessageStore store = topicStore.get(topic);
        if (store == null) {
            store = new JvmMessageStore();
            topicStore.put(topic, store);
        }*/
        store.addMessage(sendMessage);

    }


    public Message getMessage(PullMesageInfo pull) {
       /* int queueId = pull.getQueueId();
        MessageStore store = topicStore.get(pull.getTopic());
        if (store == null) {
            return null;
        }*/
        return store.getMessage(pull);
    }


    public RemotingCommand getRespone(PullMesageInfo pull){

        RemotingCommand response = RemotingCommand.createRequestCommand(ResponseCode.SUCCESS, null);

        GetMessageResult result = store.getMessageContent(pull);

        if (result != null) {
            switch (result.getStatus()) {

                default:
                    assert false;
                    break;
            }

            switch (response.getCode()) {
                case ResponseCode.SUCCESS:
                default:
                    assert false;
            }


            byte[] bytes = readGetMessageResult(result);
            response.getExtFields().put(MessageOffsetConstant.MINOFFSET, result.getMinOffset());
            response.getExtFields().put(MessageOffsetConstant.MAXOFFSET, result.getMaxOffset());
            response.getExtFields().put(MessageOffsetConstant.NEXTBEGINOFFSET, result.getNextBeginOffset());

            response.setBody(bytes);

        }else{
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
