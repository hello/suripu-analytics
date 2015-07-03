package com.hello.suripu.analytics.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

public class CheckpointTracker {
    public static final String CHECKPOINT_TRACK_KEY = "checkpoint_track";
    public static final Integer CHECKPOINT_NUM_TRACKS = 24;
    public static final Integer CHECKPOINT_TRACK_PERIOD = 2;   // in minutes

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointTracker.class);
    private final JedisPool jedisPool;
    private Long lastCheckpointTimestamp;

    public CheckpointTracker(final JedisPool jedisPool) {
        this.jedisPool = jedisPool;
        this.lastCheckpointTimestamp = 0L;
    }

    public void trackCheckpoint(final String shardId, final String sequenceNumber, final Long timestamp) {

        final Long delta = (timestamp - lastCheckpointTimestamp);
        LOGGER.debug("Checkpoint Timestamp Delta: {}", delta.toString());
        lastCheckpointTimestamp = timestamp;
        LOGGER.debug("Tracked kinesis checkpoint for shardId: {}", shardId);
    }
    public Boolean isEligibleForTracking(final Long recordTimestamp) {
        return (recordTimestamp > (lastCheckpointTimestamp + (CHECKPOINT_TRACK_PERIOD * 60000L)));
    }
}
