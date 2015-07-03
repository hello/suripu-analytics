package com.hello.suripu.analytics.processors;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hello.suripu.analytics.utils.ActiveDevicesTracker;
import com.hello.suripu.api.ble.SenseCommandProtos;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jnorgan on 6/29/15.
 */
public class PillStatsProcessor implements IRecordProcessor {

    private final static Logger LOGGER = LoggerFactory.getLogger(PillStatsProcessor.class);

    private final ActiveDevicesTracker activeDevicesTracker;
    private final Meter messagesProcessed;
    private String shardId = "No Lease Key";

    public PillStatsProcessor(final ActiveDevicesTracker activeDevicesTracker){

        this.messagesProcessed = Metrics.defaultRegistry().newMeter(PillStatsProcessor.class, "messages", "messages-processed", TimeUnit.SECONDS);
        this.activeDevicesTracker = activeDevicesTracker;
    }

    public void initialize(String shardId) {
        this.shardId = shardId;
    }

    @Timed
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {

        final Map<String, Long> activePills = Maps.newHashMap();

        for(final Record record : records) {
            try {
                final SenseCommandProtos.batched_pill_data batched_pill_data = SenseCommandProtos.batched_pill_data.parseFrom(record.getData().array());
                for (final SenseCommandProtos.pill_data data : batched_pill_data.getPillsList()) {

                    final Long pillTs = data.getTimestamp() * 1000L;
                    activePills.put(data.getDeviceId(), pillTs);
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
