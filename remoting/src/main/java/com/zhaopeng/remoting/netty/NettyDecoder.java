package com.zhaopeng.remoting.netty;


import com.zhaopeng.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by zhaopeng on 2017/3/25.
 */

/**
 * 基于包头不固定长度的解码器
 *
  maxFrameLength：解码的帧的最大长度
 lengthFieldOffset：长度属性的起始位（偏移位），包中存放有整个大数据包长度的字节，这段字节的起始位置
 lengthFieldLength：长度属性的长度，即存放整个大数据包长度的字节所占的长度
 lengthAdjustmen：长度调节值，在总长被定义为包含包头长度时，修正信息长度。
 initialBytesToStrip：跳过的字节数，根据需要我们跳过lengthFieldLength个字节，以便接收端直接接受到不含“长度属性”的内容
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static final Logger logger = LoggerFactory.getLogger(NettyDecoder.class);

    private static final int FRAME_MAX_LENGTH = 16777216;

    public NettyDecoder() {
        // 传输过程中一共四个部分,
        // 0 表示长度属性的开始索引
        // 4 长度属性所占用的字节数
        // 补偿字节数为0
        // 跳过4个字节，解码后只需要后面三个部分。
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);

    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }
            ByteBuffer byteBuffer = frame.nioBuffer();
            //解码
            return RemotingCommand.decode(byteBuffer);
        } catch (Exception e) {
            logger.error("decode exception {}" ,e);
            ctx.close();
        } finally {
            if (null != frame) {
                frame.release();
            }
        }

        return null;
    }


}
