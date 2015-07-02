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
import com.hello.suripu.api.input.DataInputProtos;
import com.hello.suripu.core.models.FirmwareInfo;
import com.hello.suripu.core.processors.OTAProcessor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jnorgan on 6/29/15.
 */
public class SenseStatsProcessor implements IRecordProcessor {

    private final static Logger LOGGER = LoggerFactory.getLogger(SenseStatsProcessor.class);

    private final ActiveDevicesTracker activeDevicesTracker;
    private String shardId = "No Lease Key";

    public SenseStatsProcessor(final ActiveDevicesTracker activeDevicesTracker){

        this.activeDevicesTracker = activeDevicesTracker;
    }

    public void initialize(String shardId) {
        this.shardId = shardId;
    }

    @Timed
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {

        final Map<String, Long> activeSenses = Maps.newHashMap();
        final Map<String, FirmwareInfo> seenFirmwares = Maps.newHashMap();

        for(final Record record : records) {
            DataInputProtos.BatchPeriodicDataWorker batchPeriodicDataWorker;
            try {
                batchPeriodicDataWorker = DataInputProtos.BatchPeriodicDataWorker.parseFrom(record.getData().array());
            } catch (InvalidProtocolBufferException e) {
                LOGGER.error("Failed parsing protobuf: {}", e.getMessage());
                LOGGER.error("Moving to next record");
                continue;
            }

            final String deviceName = batchPeriodicDataWorker.getData().getDeviceId();
            final String deviceIPAddress = batchPeriodicDataWorker.getIpAddress();

            //Filter out PCH IPs from active sense tracking
            if (!OTAProcessor.isPCH(deviceIPAddress, Collections.EMPTY_LIST)) {
                activeSenses.put(deviceName, batchPeriodicDataWorker.getReceivedAt());
            }

            final Map<Integer, Long> fwVersionTimestampMap = Maps.newHashMap();
            for(final DataInputProtos.periodic_data periodicData : batchPeriodicDataWorker.getData().getDataList()) {
                final Long timestampMillis = periodicData.getUnixTime() * 1000L;
                // Grab FW version from Batch or periodic data for EVT units
                final Integer firmwareVersion = (batchPeriodicDataWorker.getData().hasFirmwareVersion())
                        ? batchPeriodicDataWorker.getData().getFirmwareVersion()
                        : periodicData.getFirmwareVersion();
                if (fwVersionTimestampMap.containsKey(firmwareVersion) && fwVersionTimestampMap.get(firmwareVersion) > timestampMillis) {
                    continue;
                }

                fwVersionTimestampMap.put(firmwareVersion, timestampMillis);
            }

            for(final Map.Entry<Integer, Long> mapEntry : fwVersionTimestampMap.entrySet()) {
                final String firmwareVersion = mapEntry.getKey().toString();
                final Long timeStamp = mapEntry.getValue();
                seenFirmwares.put(deviceName, new FirmwareInfo(firmwareVersion, deviceName, timeStamp));
            }

            LOGGER.debug("Processed record for: {} with time: {}", deviceName, batchPeriodicDataWorker.getReceivedAt());
        }

        try {
            iRecordProcessorCheckpointer.checkpoint();
        } catch (InvalidStateException e) {
            LOGGER.error("checkpoint {}", e.getMessage());
        } catch (ShutdownException e) {
            LOGGER.error("Received shutdown command at checkpoint, bailing. {}", e.getMessage());
        }

        //LOGGER.info("Shard Id: {} Last Checkpoint: , Millis behind present: ", shardId);
        activeDevicesTracker.trackSenses(activeSenses);
        activeDevicesTracker.trackFirmwares(seenFirmwares);

    }

    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {

    }
}
