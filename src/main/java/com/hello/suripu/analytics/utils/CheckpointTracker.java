package com.hello.suripu.analytics.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.sun.org.apache.xpath.internal.operations.Bool;

import com.google.common.collect.Maps;
import java.util.Map;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointTracker {
    public static final Integer CHECKPOINT_NUM_TRACKS = 24; //TODO: Add track expiration
    public static final Long CHECKPOINT_TRACK_PERIOD = 5L;   // in minutes
    public static final String STREAM_SHARD_ATTRIBUTE_NAME = "stream_shardId";
    public static final String TIMESTAMP_ATTRIBUTE_NAME = "created_at";
    public static final String CHECKPOINT_ATTRIBUTE_NAME = "checkpoint";
    public final static String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ssZ";

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointTracker.class);
    private final AmazonDynamoDB dynamoDBClient;
    private final String streamName;
    private Long lastCheckpointTimestamp;
    private final String tableName;

    public CheckpointTracker(final AmazonDynamoDB dynamoDBClient, final String streamName, final String tableName) {
        this.dynamoDBClient = dynamoDBClient;
        this.streamName = streamName;
        this.lastCheckpointTimestamp = 0L;
        this.tableName = tableName;
    }

    public void trackCheckpoint(final String shardId, final String sequenceNumber, final Long timestamp) {

        final Long delta = (timestamp - lastCheckpointTimestamp);
        LOGGER.debug("Checkpoint Timestamp Delta: {}", delta.toString());
        lastCheckpointTimestamp = timestamp;

        insertCheckpoint(streamName + ":" + shardId, sequenceNumber, timestamp);

        LOGGER.debug("Tracked kinesis checkpoint for shardId: {}", shardId);
    }
    public Boolean isEligibleForTracking(final Long recordTimestamp) {
        if ((recordTimestamp > (lastCheckpointTimestamp + (CHECKPOINT_TRACK_PERIOD * 60000L))) &&
            recordTimestamp < (lastCheckpointTimestamp + (CHECKPOINT_TRACK_PERIOD * 60000L * 60L))) {
          return true;
        }
        return false;
    }

    public void insertCheckpoint(final String streamShardId, final String checkpoint, final Long timestamp) {

        final DateTime dateTimestamp = new DateTime(timestamp);
        final Map<String, AttributeValue> item = Maps.newHashMap();
        item.put(STREAM_SHARD_ATTRIBUTE_NAME, (new AttributeValue()).withS(streamShardId));
        item.put(TIMESTAMP_ATTRIBUTE_NAME, (new AttributeValue()).withS(dateTimestamp.toString(DATETIME_FORMAT)));
        item.put(CHECKPOINT_ATTRIBUTE_NAME, (new AttributeValue()).withS(checkpoint));
        PutItemRequest putItemRequest = new PutItemRequest(this.tableName, item);

        try {
            this.dynamoDBClient.putItem(putItemRequest);
            LOGGER.debug("Checkpoint tracked for {} at {}", streamShardId, timestamp);
        } catch (AmazonServiceException var5) {
            LOGGER.error("Checkpoint track insert failed. AWS service error: {}", var5.getMessage());
        } catch (AmazonClientException var6) {
            LOGGER.error("Checkpoint track insert failed. Client error: {}", var6.getMessage());
        }

    }

    public static CreateTableResult createTable(final String tableName, final AmazonDynamoDBClient dynamoDBClient){
        final CreateTableRequest request = new CreateTableRequest().withTableName(tableName);

        request.withKeySchema(
                new KeySchemaElement().withAttributeName(STREAM_SHARD_ATTRIBUTE_NAME).withKeyType(KeyType.HASH),
                new KeySchemaElement().withAttributeName(TIMESTAMP_ATTRIBUTE_NAME).withKeyType(KeyType.RANGE)
        );

        request.withAttributeDefinitions(
                new AttributeDefinition().withAttributeName(STREAM_SHARD_ATTRIBUTE_NAME).withAttributeType(ScalarAttributeType.S),
                new AttributeDefinition().withAttributeName(TIMESTAMP_ATTRIBUTE_NAME).withAttributeType(ScalarAttributeType.S)

        );

        request.setProvisionedThroughput(new ProvisionedThroughput()
                .withReadCapacityUnits(1L)
                .withWriteCapacityUnits(1L));

        return dynamoDBClient.createTable(request);
    }
}
