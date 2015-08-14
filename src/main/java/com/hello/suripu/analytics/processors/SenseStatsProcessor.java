package com.hello.suripu.analytics.processors;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hello.suripu.analytics.utils.ActiveDevicesTracker;
import com.hello.suripu.analytics.utils.CheckpointTracker;
import com.hello.suripu.api.input.DataInputProtos;
import com.hello.suripu.core.models.FirmwareInfo;
import com.hello.suripu.core.processors.OTAProcessor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by jnorgan on 6/29/15.
 */
public class SenseStatsProcessor implements IRecordProcessor {

    static final MetricRegistry metrics = new MetricRegistry();
    private final static Logger LOGGER = LoggerFactory.getLogger(SenseStatsProcessor.class);
    private final static Long LOW_UPTIME_THRESHOLD = 3600L; //seconds

    private final ActiveDevicesTracker activeDevicesTracker;
    private final CheckpointTracker checkpointTracker;
    private final Meter messagesProcessed = metrics.meter(name(SenseStatsProcessor.class, "messages-processed"));
    private final Meter waveCounts = metrics.meter(name(SenseStatsProcessor.class, "wave-counts"));
    private final Meter lowUptimeCount = metrics.meter(name(SenseStatsProcessor.class, "low-uptime"));
    private final Histogram uptimeDays = metrics.histogram(name(SenseStatsProcessor.class, "uptime-days"));
    private BloomFilter<CharSequence> bloomFilter;
    private Long lastFilterTimestamp;
    private String shardId = "No Lease Key";

    public SenseStatsProcessor(final ActiveDevicesTracker activeDevicesTracker, final CheckpointTracker checkpointTracker){
        this.activeDevicesTracker = activeDevicesTracker;
        this.checkpointTracker = checkpointTracker;
    }

    public void initialize(String shardId) {
        this.shardId = shardId;
        createNewBloomFilter();
    }

    private void createNewBloomFilter() {
        bloomFilter = BloomFilter.create(Funnels.stringFunnel(), 20000, 0.03);
        this.lastFilterTimestamp = DateTime.now().getMillis();
    }

    public void processRecords(List<Record> records, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {

        final Map<String, Long> activeSenses = Maps.newHashMap();
        final Map<String, FirmwareInfo> seenFirmwares = Maps.newHashMap();
        Long waveCountSum = 0L;

        if(DateTime.now(DateTimeZone.UTC).getMillis() > (lastFilterTimestamp + (LOW_UPTIME_THRESHOLD * 1000L))) {
            createNewBloomFilter();
        }

        for(final Record record : records) {

            final String sequenceNumber = record.getSequenceNumber();
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
            final Integer deviceUptime = batchPeriodicDataWorker.getUptimeInSecond();

            int days = Days.daysBetween(DateTime.now(DateTimeZone.UTC), DateTime.now(DateTimeZone.UTC).minusSeconds(deviceUptime)).getDays();

            if (deviceUptime <= LOW_UPTIME_THRESHOLD) {
                if(!bloomFilter.mightContain(deviceName)) {
                    bloomFilter.put(deviceName);
                    lowUptimeCount.mark(1);
                    uptimeDays.update(days);
                }

            }

            //Filter out PCH IPs from active sense tracking
            if (!OTAProcessor.isPCH(deviceIPAddress, Collections.EMPTY_LIST)) {
                activeSenses.put(deviceName, batchPeriodicDataWorker.getReceivedAt());
            }

            final Map<Integer, Long> fwVersionTimestampMap = Maps.newHashMap();
            for(final DataInputProtos.periodic_data periodicData : batchPeriodicDataWorker.getData().getDataList()) {
                final Integer waveCount = periodicData.getWaveCount();
                waveCountSum += waveCount;

                final Long timestampMillis = periodicData.getUnixTime() * 1000L;

                if (checkpointTracker.isEligibleForTracking(timestampMillis)) {
                    checkpointTracker.trackCheckpoint(shardId, sequenceNumber, timestampMillis);
                }

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
        }

        try {
            iRecordProcessorCheckpointer.checkpoint();
        } catch (InvalidStateException e) {
            LOGGER.error("checkpoint {}", e.getMessage());
        } catch (ShutdownException e) {
            LOGGER.error("Received shutdown command at checkpoint, bailing. {}", e.getMessage());
        }

        activeDevicesTracker.trackSenses(activeSenses);
        activeDevicesTracker.trackFirmwares(seenFirmwares);

        messagesProcessed.mark(records.size());
        waveCounts.mark(waveCountSum);

    }

    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {

    }
}
