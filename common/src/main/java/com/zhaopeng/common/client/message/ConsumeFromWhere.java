
package com.zhaopeng.common.client.message;


/**
 *   消费记录的offset，如果此mq已经被消费过，broker端就会记录 此mq的消费记录
 *   如果此mq是第一次被消费就，有三种策略:
 *   CONSUME_FROM_LAST_OFFSET : 从mq的最后的位置开始消费
 *   CONSUME_FROM_FIRST_OFFSET: 从队列开始的位置消费
 *   CONSUME_FROM_TIMESTAMP: 从指定时间点开始消费
 *
 */
public enum ConsumeFromWhere {

    CONSUME_FROM_LAST_OFFSET,
    CONSUME_FROM_FIRST_OFFSET,
    CONSUME_FROM_TIMESTAMP;
}
