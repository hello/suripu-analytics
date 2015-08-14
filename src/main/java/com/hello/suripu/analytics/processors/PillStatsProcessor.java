package com.hello.suripu.analytics.processors;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hello.suripu.analytics.utils.ActiveDevicesTracker;
import com.hello.suripu.analytics.utils.CheckpointTracker;
import com.hello.suripu.api.ble.SenseCommandProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by jnorgan on 6/29/15.
 */
public class PillStatsProcessor implements IRecordProcessor {

    private final static Logger LOGGER = LoggerFactory.getLogger(PillStatsProcessor.class);
    static final MetricRegistry metrics = new MetricRegistry();
    private final ActiveDevicesTracker activeDevicesTracker;
    private final CheckpointTracker checkpointTracker;
    private final Meter messagesProcessed = metrics.meter(name(PillStatsProcessor.class, "messages-processed"));
    private String shardId = "No Lease Key";

    public PillStatsProcessor(final ActiveDevicesTracker activeDevicesTracker, final CheckpointTracker checkpointTracker){
        this.activeDevicesTracker = activeDevicesTracker;
        this.checkpointTracker = checkpointTracker;
    }

    public void initialize(String shardId) {
        this.shardId = shardId;
    }

    @Timed
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {

        final Map<String, Long> activePills = Maps.newHashMap();

        for(final Record record : records) {
            final String sequenceNumber = record.getSequenceNumber();

            try {
                final SenseCommandProtos.batched_pill_data batched_pill_data = SenseCommandProtos.batched_pill_data.parseFrom(record.getData().array());
                for (final SenseCommandProtos.pill_data data : batched_pill_data.getPillsList()) {

                    final Long pillTs = data.getTimestamp() * 1000L;
                    activePills.put(data.getDeviceId(), pillTs);

                    if (checkpointTracker.isEligibleForTracking(pillTs)) {
                        checkpointTracker.trackCheckpoint(shardId, sequenceNumber, pillTs);
                    }
                }
            } catch (InvalidProtocolBufferException e) {
                LOGGER.error("Failed to decode protobuf: {}", e.getMessage());
            } catch (IllegalArgumentException e) {
                LOGGER.error("Failed to decrypted pill data {}, error: {}", record.getData().array(), e.getMessage());
            }
        }

        try {
            iRecordProcessorCheckpointer.checkpoint();
        } catch (InvalidStateException e) {
            LOGGER.error("checkpoint {}", e.getMessage());
        } catch (ShutdownException e) {
            LOGGER.error("Received shutdown command at checkpoint, bailing. {}", e.getMessage());
        }

        activeDevicesTracker.trackPills(activePills);
        messagesProcessed.mark(records.size());
    }

    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {

    }
}
