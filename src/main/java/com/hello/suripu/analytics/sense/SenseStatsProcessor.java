package com.hello.suripu.analytics.sense;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hello.suripu.analytics.utils.ActiveDevicesTracker;
import com.hello.suripu.api.input.DataInputProtos;
import com.hello.suripu.core.db.MergedUserInfoDynamoDB;
import com.hello.suripu.core.models.FirmwareInfo;
import com.hello.suripu.core.models.UserInfo;
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
    private final MergedUserInfoDynamoDB mergedInfoDynamoDB;

    public SenseStatsProcessor(final MergedUserInfoDynamoDB mergedInfoDynamoDB, final ActiveDevicesTracker activeDevicesTracker){

        this.mergedInfoDynamoDB = mergedInfoDynamoDB;
        this.activeDevicesTracker = activeDevicesTracker;
    }

    public void initialize(String s) {

    }

    @Timed
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {

        final Map<String, Long> activeSenses = Maps.newHashMap();
        final Map<String, FirmwareInfo> seenFirmwares = Maps.newHashMap();
        final Map<String, Long> allSeenSenses = Maps.newHashMap();

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

            //Logging seen device before attempting account pairing
            allSeenSenses.put(deviceName, batchPeriodicDataWorker.getReceivedAt());

            // This is the default timezone.
            final List<UserInfo> deviceAccountInfoFromMergeTable = Lists.newArrayList();
            int retries = 2;
            for(int i = 0; i < retries; i++) {
                try {
                    deviceAccountInfoFromMergeTable.addAll(this.mergedInfoDynamoDB.getInfo(deviceName));  // get everything by one hit
                    break;
                } catch (AmazonClientException exception) {
                    LOGGER.error("Failed getting info from DynamoDB for device = {}", deviceName);
                }

                try {
                    LOGGER.warn("Sleeping for 1 sec");
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    LOGGER.warn("Thread sleep interrupted");
                }
                retries++;
            }

            if(deviceAccountInfoFromMergeTable.isEmpty()) {
                LOGGER.warn("Device {} is not stored in DynamoDB or doesn't have any accounts linked.", deviceName);
            } else { // track only for sense paired to accounts
                activeSenses.put(deviceName, batchPeriodicDataWorker.getReceivedAt());
            }

            for(final DataInputProtos.periodic_data periodicData : batchPeriodicDataWorker.getData().getDataList()) {
                final Long timestampMillis = periodicData.getUnixTime() * 1000L;
                // Grab FW version from Batch or periodic data for EVT units
                final Integer firmwareVersion = (batchPeriodicDataWorker.getData().hasFirmwareVersion())
                        ? batchPeriodicDataWorker.getData().getFirmwareVersion()
                        : periodicData.getFirmwareVersion();

                seenFirmwares.put(deviceName, new FirmwareInfo(firmwareVersion.toString(), deviceName, timestampMillis));
            }

            LOGGER.debug("Processed record for: {}", deviceName);
        }




        try {
            iRecordProcessorCheckpointer.checkpoint();
        } catch (InvalidStateException e) {
            LOGGER.error("checkpoint {}", e.getMessage());
        } catch (ShutdownException e) {
            LOGGER.error("Received shutdown command at checkpoint, bailing. {}", e.getMessage());
        }


        activeDevicesTracker.trackAllSeenSenses(allSeenSenses);
        activeDevicesTracker.trackSenses(activeSenses);
        activeDevicesTracker.trackFirmwares(seenFirmwares);

    }

    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {

    }
}
