package com.zhaopeng.store.disk;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhaopeng on 2017/8/13.
 */
public class GetMessageResult {


    private final List<SelectMapedBufferResult> messageMapedList =
            new ArrayList<>(100);

    private final List<ByteBuffer> messageBufferList = new ArrayList<ByteBuffer>(100);

    private int status;
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;

    private int bufferTotalSize = 0;

    private boolean suggestPullingFromSlave = false;

    private int msgCount4Commercial = 0;


}
