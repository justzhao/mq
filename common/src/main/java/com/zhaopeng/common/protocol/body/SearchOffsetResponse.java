package com.zhaopeng.common.protocol.body;

import com.zhaopeng.remoting.protocol.JsonSerializable;

/**
 * Created by zhaopeng on 2017/5/2.
 */
public class SearchOffsetResponse  extends JsonSerializable {

    private Long offset;

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }
}
