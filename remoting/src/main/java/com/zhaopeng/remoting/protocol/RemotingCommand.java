package com.zhaopeng.remoting.protocol;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.collect.Maps;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.zhaopeng.remoting.protocol.SerializeType.JSON;
import static com.zhaopeng.remoting.protocol.SerializeType.ROCKETMQ;

/**
 * Created by zhaopeng on 2017/3/25.
 */
public class RemotingCommand {


    private static final int RPC_TYPE = 0; // 0 是req 1 是 rep
    private static final int RPC_ONEWAY = 1; // 0  rpc 需要返回结果, 1 ,不需要返回结果
    /**
     *  消息的个数信息等
     *
     */

    private Long nextBeginOffset;

    private Long minOffset;

    private Long maxOffset;
    /**
     * 静态变量用来唯一保存 请求id
     */
    private static AtomicInteger requestId = new AtomicInteger(0);

    // body
    private transient byte[] body;

    /**
     * 下面参数需要被序列化传输
     */

    // 业务类型
    private int code;

    private String remark;
    //标识 RPC的方式
    private int flag = 0;
    /**
     * 扩展字段
     */
    private HashMap<String, String> extFields = Maps.newHashMap();

    /**
     * 用来临时保存 requestId
     */
    private int opaque = requestId.getAndIncrement();

    /**
     * 序列化字段结束
     */


    private static SerializeType serializeType = JSON;


    /**
     * 获取最新的requestId
     *
     * @return
     */
    public static int createNewRequestId() {
        return requestId.incrementAndGet();
    }


    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    /**
     * 根据body的数据长度产生包头数据
     *
     * @param bodyLength
     * @return
     */
    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1> header length size , 消息头和消息体的总数据长度
        int length = 4;

        // 2> header data length  消息头数据的长度
        byte[] headerData;
        headerData = this.headerEncode();

        length += headerData.length;

        // 3> body data length
        length += bodyLength;
        //分配一个4 字节+ 包头长度的缓存
        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // length  int 长度为4个字节
        result.putInt(length);

        // header length  消息头的长度
        result.put(markProtocolType(headerData.length, serializeType));

        // header data
        result.put(headerData);

        result.flip();

        return result;
    }

    public static byte[] markProtocolType(int source, SerializeType type) {
        byte[] result = new byte[4];

        result[0] = type.getCode();
        result[1] = (byte) ((source >> 16) & 0xFF);
        result[2] = (byte) ((source >> 8) & 0xFF);
        result[3] = (byte) (source & 0xFF);
        return result;
    }

    private byte[] headerEncode() {
        if (serializeType == ROCKETMQ) {
            return MQSerializable.mqProtocolEncode(this);
        } else {
            return JsonSerializable.encode(this);
        }
    }

    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }

    public boolean isOneWay() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }

    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }

        return RemotingCommandType.REQUEST_COMMAND;
    }


    public static RemotingCommand createResponseCommand(int code, String remark) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setRemark(remark);
        cmd.markResponseType();
        return cmd;
    }

    public static RemotingCommand createRequestCommand(int code, String remark) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setRemark(remark);
        return cmd;
    }

    public static RemotingCommand decode(final ByteBuffer byteBuffer) {
        //length = 存放消息头数据的有多长的所占用字节数+ 消息头数据占用字节数+body数据的占用字节数
        int length = byteBuffer.limit();
        //oriHeaderlen包含 消息头的长度和序列化的方式
        int oriHeaderLen = byteBuffer.getInt();
        // 获取到消息头的长度
        int headerLength = getHeaderLength(oriHeaderLen);
        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);
        RemotingCommand cmd = headerDecode(headerData, getProtocolType(oriHeaderLen));

        // body的长度
        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.get(bodyData);
        }
        cmd.body = bodyData;

        return cmd;
    }

    public Long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(Long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public Long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(Long minOffset) {
        this.minOffset = minOffset;
    }

    public Long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(Long maxOffset) {
        this.maxOffset = maxOffset;
    }

    /**
     * 一共4 个byte。 最高的byte存放序列化的方式
     *
     * @param source
     * @return
     */
    public static SerializeType getProtocolType(int source) {
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }

    public static int getHeaderLength(int length) {
        return length & 0xFFFFFF;
    }

    private static RemotingCommand headerDecode(byte[] headerData, SerializeType type) {
        switch (type) {
            case JSON:
                RemotingCommand resultJson = JsonSerializable.decode(headerData, RemotingCommand.class);
                resultJson.setSerializeType(type);
                return resultJson;
            case ROCKETMQ:
                RemotingCommand resultRMQ = MQSerializable.rocketMQProtocolDecode(headerData);
                resultRMQ.setSerializeType(type);
                return resultRMQ;
            default:
                break;
        }

        return null;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }


    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public static SerializeType getSerializeType() {
        return serializeType;
    }

    public static void setSerializeType(SerializeType serializeType) {
        RemotingCommand.serializeType = serializeType;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }


    public HashMap<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }


    @Override
    public String toString() {
        return "RemotingCommand{" +
                "body=" + Arrays.toString(body) +
                ", code=" + code +
                ", remark='" + remark + '\'' +
                ", flag=" + flag +
                ", extFields=" + extFields +
                ", opaque=" + opaque +
                '}';
    }
}
