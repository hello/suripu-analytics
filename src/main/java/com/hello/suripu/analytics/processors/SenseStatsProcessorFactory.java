package com.hello.suripu.analytics.processors;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.hello.suripu.analytics.utils.ActiveDevicesTracker;
import com.hello.suripu.analytics.utils.CheckpointTracker;
import redis.clients.jedis.JedisPool;

/**
 * Created by jnorgan on 6/29/15.
 */
public class SenseStatsProcessorFactory implements IRecordProcessorFactory {

    private final JedisPool jedisPool;
    private final AmazonDynamoDB dynamoDBClient;
    private final String appName;

    public SenseStatsProcessorFactory(final JedisPool jedisPool, final AmazonDynamoDB dynamoDBClient, final String appName) {
        this.jedisPool = jedisPool;
        this.dynamoDBClient = dynamoDBClient;
        this.appName = appName;
    }

    public IRecordProcessor createProcessor() {
        final ActiveDevicesTracker activeDevicesTracker = new ActiveDevicesTracker(jedisPool);
        final CheckpointTracker checkpointTracker = new CheckpointTracker(dynamoDBClient, appName);
        return new SenseStatsProcessor(activeDevicesTracker, checkpointTracker);
    }
}
