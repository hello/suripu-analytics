package com.hello.suripu.analytics.processors;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Maps;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hello.suripu.analytics.models.WifiInfo;
import com.hello.suripu.analytics.utils.ActiveDevicesTracker;
import com.hello.suripu.api.input.DataInputProtos;
import com.hello.suripu.core.models.FirmwareInfo;
import com.hello.suripu.core.processors.OTAProcessor;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by jnorgan on 6/29/15.
 */
public class SenseStatsProcessor implements IRecordProcessor {

    private final static Logger LOGGER = LoggerFactory.getLogger(SenseStatsProcessor.class);
    private final static Long LOW_UPTIME_THRESHOLD = 3600L; //seconds

    private final ActiveDevicesTracker activeDevicesTracker;
    private final Meter messagesProcessed;
    private final Meter waveCounts;
    private final Meter lowUptimeCount;
    private BloomFilter<CharSequence> bloomFilter;
    private Long lastFilterTimestamp;
    private String shardId = "No Lease Key";

    public SenseStatsProcessor(final ActiveDevicesTracker activeDevicesTracker){

        this.messagesProcessed = Metrics.defaultRegistry().newMeter(SenseStatsProcessor.class, "messages", "messages-processed", TimeUnit.SECONDS);
        this.waveCounts = Metrics.defaultRegistry().newMeter(SenseStatsProcessor.class, "waves", "wave-counts", TimeUnit.SECONDS);
        this.lowUptimeCount = Metrics.defaultRegistry().newMeter(SenseStatsProcessor.class, "lowuptime", "low-uptime", TimeUnit.SECONDS);
        this.activeDevicesTracker = activeDevicesTracker;
    }

    public void initialize(String shardId) {
        this.shardId = shardId;
        createNewBloomFilter();
    }

    private void createNewBloomFilter() {
        bloomFilter = BloomFilter.create(Funnels.stringFunnel(), 20000, 0.03);
        this.lastFilterTimestamp = DateTime.now().getMillis();
    }

    @Timed
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {

        final Map<String, Long> activeSenses = Maps.newHashMap();
        final Map<String, FirmwareInfo> seenFirmwares = Maps.newHashMap();
        final Map<String, WifiInfo> wifiInfos = Maps.newHashMap();

        Long waveCountSum = 0L;

        if(DateTime.now(DateTimeZone.UTC).getMillis() > (lastFilterTimestamp + (LOW_UPTIME_THRESHOLD * 1000L))) {
            createNewBloomFilter();
        }
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
            final Integer deviceUptime = batchPeriodicDataWorker.getUptimeInSecond();

            if (deviceUptime <= LOW_UPTIME_THRESHOLD) {
                if(!bloomFilter.mightContain(deviceName)) {
                    bloomFilter.put(deviceName);
                    lowUptimeCount.mark(1);
                }

            }

            //Filter out PCH IPs from active sense tracking
            if (!OTAProcessor.isPCH(deviceIPAddress, Collections.EMPTY_LIST)) {
                activeSenses.put(deviceName, batchPeriodicDataWorker.getReceivedAt());
            }

            final Map<Integer, Long> fwVersionTimestampMap = Maps.newHashMap();

            final DataInputProtos.batched_periodic_data batchedPeriodicData = batchPeriodicDataWorker.getData();
            final String connectedSSID = batchedPeriodicData.getConnectedSsid();

            Integer rssi = 0;
            for (final DataInputProtos.batched_periodic_data.wifi_access_point wifiAccessPoint : batchedPeriodicData.getScanList()) {
                if (connectedSSID.equals(wifiAccessPoint.getSsid())) {
                    rssi = wifiAccessPoint.getRssi();
                }
            }

            for(final DataInputProtos.periodic_data periodicData : batchedPeriodicData.getDataList()) {
                final Integer waveCount = periodicData.getWaveCount();
                waveCountSum += waveCount;

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
            wifiInfos.put(deviceName, new WifiInfo(rssi, connectedSSID));
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
        activeDevicesTracker.trackWifiInfo(wifiInfos);

        messagesProcessed.mark(records.size());
        waveCounts.mark(waveCountSum);

    }

    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {

    }
}
