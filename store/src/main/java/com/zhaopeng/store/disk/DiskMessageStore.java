package com.zhaopeng.store.disk;

import com.zhaopeng.common.client.message.Message;
import com.zhaopeng.common.client.message.SendMessage;
import com.zhaopeng.common.protocol.body.PullMesageInfo;
import com.zhaopeng.remoting.common.ServiceThread;
import com.zhaopeng.store.ConsumeQueue;
import com.zhaopeng.store.MessageStore;
import com.zhaopeng.store.commit.CommitLog;
import com.zhaopeng.store.config.MessageStoreConfig;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaopeng on 2017/7/27.
 */
public class DiskMessageStore implements MessageStore {


    private final MessageStoreConfig messageStoreConfig;
    // CommitLog
    private final CommitLog commitLog;

    private final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;


    private final FlushConsumeQueueService flushConsumeQueueService;

    private final CleanCommitLogService cleanCommitLogService;

    private final CleanConsumeQueueService cleanConsumeQueueService;

    private final IndexService indexService;

    public void DiskMessageStore() {

    }

    public DiskMessageStore(final MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
        commitLog = new CommitLog();
        consumeQueueTable = new ConcurrentHashMap<>();
        indexService = new IndexService();
        cleanConsumeQueueService = new CleanConsumeQueueService();
        cleanCommitLogService = new CleanCommitLogService();
        flushConsumeQueueService = new FlushConsumeQueueService();


    }


    @Override
    public Message getMessage(PullMesageInfo pull) {
        return null;
    }

    @Override
    public void addMessage(SendMessage sendMessage) {

    }

    class FlushConsumeQueueService extends ServiceThread {
        @Override
        public String getServiceName() {
            return "FlushConsumeQueueService";
        }

        @Override
        public void run() {

        }
    }

    class CleanCommitLogService extends ServiceThread {

        @Override
        public String getServiceName() {
            return "CleanCommitLogService";
        }

        @Override
        public void run() {

        }
    }

    class CleanConsumeQueueService extends ServiceThread {

        @Override
        public String getServiceName() {
            return "CleanConsumeQueueService";
        }

        @Override
        public void run() {

        }
    }

    class IndexService extends ServiceThread {

        @Override
        public String getServiceName() {
            return "IndexService";
        }

        @Override
        public void run() {

        }
    }


}
