package com.zhaopeng.common.message;

import com.zhaopeng.common.UtilAll;

import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.zhaopeng.remoting.protocol.JsonSerializable.CHARSET_UTF8;

/**
 * 二进制消息解码器
 * <p>
 * Created by zhaopeng on 2017/9/12.
 */
public class MessageDecoder {


    public static List<PullMessage> decodes(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        return decodes(byteBuffer, true);
    }

    public static List<PullMessage> decodes(java.nio.ByteBuffer byteBuffer, final boolean readBody) {
        List<PullMessage> msgs = new ArrayList<>();
        while (byteBuffer.hasRemaining()) {
            PullMessage msg = decode(byteBuffer, readBody);
            if (null != msg) {
                msgs.add(msg);
            } else {
                break;
            }
        }
        return msgs;
    }


    public static PullMessage decode(java.nio.ByteBuffer byteBuffer, final boolean readBody) {
        try {
            PullMessage msg = new PullMessage();
            // 1 TOTALSIZE
            int storeSize = byteBuffer.getInt();
            msg.setSize(storeSize);

            // 2 MAGICCODE
            byteBuffer.getInt();

            // 3 BODYCRC
            int bodyCRC = byteBuffer.getInt();
            msg.setCrc(bodyCRC);

            // 4 QUEUEID
            int queueId = byteBuffer.getInt();
            msg.setQueueId(queueId);


            // 6 QUEUEOFFSET
            long queueOffset = byteBuffer.getLong();
            msg.setQueueOffset(queueOffset);

            // 7 PHYSICALOFFSET
            long physicOffset = byteBuffer.getLong();
            msg.setPhyOffset(physicOffset);


            // 11 STORETIMESTAMP
            long storeTimestamp = byteBuffer.getLong();
            msg.setStoreTime(storeTimestamp);


            // 15 BODY
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byte[] body = new byte[bodyLen];
                    byteBuffer.get(body);
                    body = UtilAll.uncompress(body);
                    msg.setBody(body);
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            // 16 TOPIC
            byte topicLen = byteBuffer.get();
            byte[] topic = new byte[(int) topicLen];
            byteBuffer.get(topic);
            msg.setTopic(new String(topic, CHARSET_UTF8));

            return msg;
        } catch (UnknownHostException e) {
            byteBuffer.position(byteBuffer.limit());
        } catch (BufferUnderflowException e) {
            byteBuffer.position(byteBuffer.limit());
        } catch (Exception e) {
            byteBuffer.position(byteBuffer.limit());
        }

        return null;
    }

}
