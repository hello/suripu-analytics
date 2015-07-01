package com.hello.suripu.analytics.sense;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.hello.suripu.analytics.utils.ActiveDevicesTracker;
import com.hello.suripu.core.db.MergedUserInfoDynamoDB;
import redis.clients.jedis.JedisPool;

/**
 * Created by jnorgan on 6/29/15.
 */
public class SenseStatsProcessorFactory implements IRecordProcessorFactory {

    private final JedisPool jedisPool;
    private final MergedUserInfoDynamoDB mergedUserInfoDynamoDB;

    public SenseStatsProcessorFactory(final MergedUserInfoDynamoDB mergedUserInfoDynamoDB,
                                      final JedisPool jedisPool) {
        this.mergedUserInfoDynamoDB = mergedUserInfoDynamoDB;
        this.jedisPool = jedisPool;
    }

    public IRecordProcessor createProcessor() {
        final ActiveDevicesTracker activeDevicesTracker = new ActiveDevicesTracker(jedisPool);
        return new SenseStatsProcessor(mergedUserInfoDynamoDB, activeDevicesTracker);
    }
}
