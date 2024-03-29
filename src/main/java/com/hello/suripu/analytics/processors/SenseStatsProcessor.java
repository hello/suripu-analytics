package com.hello.suripu.analytics.processors;

import com.google.common.collect.Maps;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.protobuf.InvalidProtocolBufferException;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.hello.suripu.analytics.models.WifiInfo;
import com.hello.suripu.analytics.utils.ActiveDevicesTracker;
import com.hello.suripu.analytics.utils.CheckpointTracker;
import com.hello.suripu.analytics.utils.DataQualityTracker;
import com.hello.suripu.api.input.DataInputProtos;
import com.hello.suripu.core.models.FirmwareInfo;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;


/**
 * Created by jnorgan on 6/29/15.
 */
public class SenseStatsProcessor implements IRecordProcessor {

    private final MetricRegistry metrics;
    private final static Logger LOGGER = LoggerFactory.getLogger(SenseStatsProcessor.class);
    private final static Long LOW_UPTIME_THRESHOLD = 3600L; //seconds
    private final static String DEFAULT_SSID = "";
    private final static Integer DEFAULT_RSSI = 0;

    private final ActiveDevicesTracker activeDevicesTracker;
    private final CheckpointTracker checkpointTracker;
    private final Meter messagesProcessed;
    private final Meter waveCounts;
    private final Meter lowUptimeCount;
    private final Histogram uptimeDays;

    private final DataQualityTracker dataQualityTracker;

    private BloomFilter<CharSequence> bloomFilter;
    private Long lastFilterTimestamp;
    private String shardId = "No Lease Key";


    public SenseStatsProcessor(final ActiveDevicesTracker activeDevicesTracker, final CheckpointTracker checkpointTracker, final MetricRegistry metricRegistry, final DataQualityTracker dataQualityTracker){
        this.activeDevicesTracker = activeDevicesTracker;
        this.checkpointTracker = checkpointTracker;
        this.metrics= metricRegistry;
        this.dataQualityTracker = dataQualityTracker;
        messagesProcessed = metrics.meter(name(SenseStatsProcessor.class, "messages-processed"));
        waveCounts = metrics.meter(name(SenseStatsProcessor.class, "wave-counts"));
        lowUptimeCount = metrics.meter(name(SenseStatsProcessor.class, "low-uptime"));
        uptimeDays = metrics.histogram(name(SenseStatsProcessor.class, "uptime-days"));
    }

    public void initialize(String shardId) {
        this.shardId = shardId;
        createNewBloomFilter();
    }

    private void createNewBloomFilter() {
        bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 20000, 0.03);
        this.lastFilterTimestamp = DateTime.now().getMillis();
    }

    public void processRecords(List<Record> records, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {

        final Map<String, Long> activeSenses = Maps.newHashMap();
        final Map<String, FirmwareInfo> seenFirmwares = Maps.newHashMap();
        final Map<String, WifiInfo> wifiInfos = Maps.newHashMap();
        final Map<String, Integer> uptimeBySense = Maps.newHashMapWithExpectedSize(records.size());

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
                LOGGER.error("error=protobuf-parsing-failure message={}", e.getMessage());
                continue;
            }

            final String deviceName = batchPeriodicDataWorker.getData().getDeviceId();
            final String deviceIPAddress = batchPeriodicDataWorker.getIpAddress();
            final Integer deviceUptime = batchPeriodicDataWorker.getUptimeInSecond();

            uptimeBySense.put(deviceName, deviceUptime);
            int days = Days.daysBetween(DateTime.now(DateTimeZone.UTC).minusSeconds(deviceUptime), DateTime.now(DateTimeZone.UTC)).getDays();

            if (deviceUptime <= LOW_UPTIME_THRESHOLD) {
                if(!bloomFilter.mightContain(deviceName)) {
                    bloomFilter.put(deviceName);
                    lowUptimeCount.mark(1);
                }

            }

            uptimeDays.update(days);

            //Filter out PCH IPs from active sense tracking
            activeSenses.put(deviceName, batchPeriodicDataWorker.getReceivedAt());

            final Map<String, FirmwareInfo> fwVersionTimestampMap = Maps.newHashMap();

            final DataInputProtos.batched_periodic_data batchedPeriodicData = batchPeriodicDataWorker.getData();

            final String connectedSSID = batchedPeriodicData.hasConnectedSsid() ? batchedPeriodicData.getConnectedSsid() : DEFAULT_SSID;

            Integer rssi = DEFAULT_RSSI;

            for (final DataInputProtos.batched_periodic_data.wifi_access_point wifiAccessPoint : batchedPeriodicData.getScanList()) {
                if (connectedSSID.equals(wifiAccessPoint.getSsid())) {
                    rssi = wifiAccessPoint.getRssi();
                    break;
                }
            }

            if (!DEFAULT_SSID.equals(connectedSSID) && !DEFAULT_RSSI.equals(rssi)) {
                LOGGER.trace("{} {} {}", batchedPeriodicData.getDeviceId(), connectedSSID, rssi);
            }

            for(final DataInputProtos.periodic_data periodicData : batchedPeriodicData.getDataList()) {
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

                final String fwDecString = Integer.toString(firmwareVersion);
                if (fwVersionTimestampMap.containsKey(fwDecString) && fwVersionTimestampMap.get(fwDecString).timestamp > timestampMillis) {
                    continue;
                }

                fwVersionTimestampMap.put(fwDecString, new FirmwareInfo(fwDecString, "0", deviceName, timestampMillis));
            }

            //If we're getting top fw info from the protobuf, only store that
            if (batchPeriodicDataWorker.hasFirmwareTopVersion() && !batchPeriodicDataWorker.getFirmwareTopVersion().equals("0")) {
                final String topFWVersion = batchPeriodicDataWorker.getFirmwareTopVersion();
                final String middleFWVersion = batchPeriodicDataWorker.getFirmwareMiddleVersion();
                fwVersionTimestampMap.clear();
                fwVersionTimestampMap.put(middleFWVersion, new FirmwareInfo(middleFWVersion, topFWVersion, deviceName, batchPeriodicDataWorker.getReceivedAt()));
            } else {
                LOGGER.error("error=no-top-fw sense_id={}", deviceName);
            }

            for(final Map.Entry<String, FirmwareInfo> mapEntry : fwVersionTimestampMap.entrySet()) {
                seenFirmwares.put(deviceName, mapEntry.getValue());
            }

            wifiInfos.put(deviceName, new WifiInfo(rssi, connectedSSID));

            //Track data quality
            dataQualityTracker.trackDataQuality(batchedPeriodicData);


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
        activeDevicesTracker.trackWifiInfo(wifiInfos);
        activeDevicesTracker.trackUptime(uptimeBySense);

        messagesProcessed.mark(records.size());
        waveCounts.mark(waveCountSum);

    }

    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {

        LOGGER.warn("SHUTDOWN: {}", shutdownReason.toString());
        if(shutdownReason.equals(ShutdownReason.TERMINATE)) {
            LOGGER.warn("Going to checkpoint");
            try {
                iRecordProcessorCheckpointer.checkpoint();
                LOGGER.warn("Checkpointed successfully");
            } catch (InvalidStateException e) {
                LOGGER.error(e.getMessage());
            } catch (ShutdownException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }
}
