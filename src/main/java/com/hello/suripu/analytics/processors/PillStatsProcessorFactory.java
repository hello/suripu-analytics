package com.hello.suripu.analytics.processors;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.codahale.metrics.MetricRegistry;
import com.hello.suripu.analytics.utils.ActiveDevicesTracker;
import com.hello.suripu.analytics.utils.CheckpointTracker;
import redis.clients.jedis.JedisPool;

/**
 * Created by jnorgan on 6/29/15.
 */
public class PillStatsProcessorFactory implements IRecordProcessorFactory {

    private final JedisPool jedisPool;
    private final AmazonDynamoDB dynamoDBClient;
    private final String streamName;
    private final String checkpointTableName;
    private final MetricRegistry metricRegistry;

    public PillStatsProcessorFactory(final JedisPool jedisPool, final AmazonDynamoDB dynamoDBClient, final String streamName, final String checkpointTableName, final MetricRegistry metricRegistry) {
        this.jedisPool = jedisPool;
        this.dynamoDBClient = dynamoDBClient;
        this.streamName = streamName;
        this.checkpointTableName = checkpointTableName;
        this.metricRegistry = metricRegistry;
    }

    public IRecordProcessor createProcessor() {
        final ActiveDevicesTracker activeDevicesTracker = new ActiveDevicesTracker(jedisPool);
        final CheckpointTracker checkpointTracker = new CheckpointTracker(dynamoDBClient, streamName, checkpointTableName);
        return new PillStatsProcessor(activeDevicesTracker, checkpointTracker, metricRegistry);
    }
}
