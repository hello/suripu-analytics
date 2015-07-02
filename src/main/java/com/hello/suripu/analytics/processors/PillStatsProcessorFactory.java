package com.hello.suripu.analytics.processors;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.hello.suripu.analytics.utils.ActiveDevicesTracker;
import redis.clients.jedis.JedisPool;

/**
 * Created by jnorgan on 6/29/15.
 */
public class PillStatsProcessorFactory implements IRecordProcessorFactory {

    private final JedisPool jedisPool;

    public PillStatsProcessorFactory(final JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public IRecordProcessor createProcessor() {
        final ActiveDevicesTracker activeDevicesTracker = new ActiveDevicesTracker(jedisPool);
        return new PillStatsProcessor(activeDevicesTracker);
    }
}
