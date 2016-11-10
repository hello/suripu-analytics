package com.hello.suripu.analytics.utils;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.hello.suripu.analytics.processors.SenseStatsProcessor;
import com.hello.suripu.api.input.DataInputProtos;
import org.slf4j.Logger;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by jyfan on 11/9/16.
 */
public class DataQualityTracker {

    private final Logger LOGGER;

    //1.5
    private static final int LOW_CO2_THRESHOLD = 400;
    private static final int HIGH_CO2_THRESHOLD = 3500; //in ppm
    private static final int LOW_PRESSURE_THRESHOLD = 900 * 256 * 100; //in mbar
    private static final int HIGH_PRESSURE_THRESHOLD = 1000 * 256 * 100; //in mbar
    private static final int HIGH_UV_THRESHOLD = 6;

    //1.0
    private static final int HIGH_DUST_THRESHOLD = 250 * 6; //6 is roughly density to raw conversion, rounded up
    private static final int HIGH_LUX_THRESHOLD = 2000;
    private static final int HIGH_HUMIDITY_THRESHOLD = 99 * 100;
    private static final int LOW_HUMIDITY_THRESHOLD = 1 * 100;
    private static final int HIGH_TEMP_THRESHOLD = 100 * 100; // in deg C
    private static final int LOW_TEMP_THRESHOLD = 0 * 100; // in deg C
    private static final int HIGH_NOISE_THRESHOLD = (150 + 40 ) * 100;

    private final Meter lowco2;
    private final Meter highco2;
    private final Meter lowpa;
    private final Meter highpa;
    private final Meter highuv;
    private final Meter highdust;
    private final Meter highlux;
    private final Meter lowhum;
    private final Meter highhum;
    private final Meter lowtmp;
    private final Meter hightmp;
    private final Meter highdb;

    public DataQualityTracker(final MetricRegistry metrics, final Logger LOGGER) {
        this.LOGGER = LOGGER;
        lowco2 = metrics.meter(name(SenseStatsProcessor.class, "low-co2"));
        highco2 = metrics.meter(name(SenseStatsProcessor.class, "high-co2"));
        lowpa = metrics.meter(name(SenseStatsProcessor.class, "low-pa"));
        highpa = metrics.meter(name(SenseStatsProcessor.class, "high-pa"));
        highuv = metrics.meter(name(SenseStatsProcessor.class, "low-uv"));
        highdust = metrics.meter(name(SenseStatsProcessor.class, "high-uv"));
        highlux = metrics.meter(name(SenseStatsProcessor.class, "high-lux"));
        lowhum = metrics.meter(name(SenseStatsProcessor.class, "low-hum"));
        highhum = metrics.meter(name(SenseStatsProcessor.class, "high-hum"));
        lowtmp = metrics.meter(name(SenseStatsProcessor.class, "low-tmp"));
        hightmp = metrics.meter(name(SenseStatsProcessor.class, "high-tmp"));
        highdb = metrics.meter(name(SenseStatsProcessor.class, "high-db"));
    }

    public void trackDataQuality(final DataInputProtos.batched_periodic_data batchedPeriodicData) {

        final String device_id = batchedPeriodicData.getDeviceId();
        final Integer fw_version = batchedPeriodicData.getFirmwareVersion();

        for (final DataInputProtos.periodic_data periodic_data : batchedPeriodicData.getDataList()) {

            final Integer co2 = periodic_data.getCo2();
            final Integer pa = periodic_data.getPressure();
            final Integer uv = periodic_data.getLightSensor().getUvCount();

            final Integer dust = periodic_data.getDust();
            final Integer uvlux = periodic_data.getLightSensor().getLuxCount();
            final Integer clearlux = periodic_data.getLightSensor().getClear();
            final Integer hum = periodic_data.getHumidity();
            final Integer tmp = periodic_data.getTemperature();
            final Integer db = periodic_data.getAudioPeakBackgroundEnergyDb();

            if (co2 < LOW_CO2_THRESHOLD) {
                lowco2.mark();
                LOGGER.error("bad_sensor=co2 sensor_val={} device_id={} fw_version={}", co2, device_id, fw_version);
            } else if (co2 > HIGH_CO2_THRESHOLD) {
                highco2.mark();
                LOGGER.error("bad_sensor=co2 sensor_val={} device_id={} fw_version={}", co2, device_id, fw_version);
            }

            if (pa < LOW_PRESSURE_THRESHOLD) {
                lowpa.mark();
                LOGGER.error("bad_sensor=pa sensor_val={} device_id={} fw_version={}", pa, device_id, fw_version);

            } else if (pa > HIGH_PRESSURE_THRESHOLD) {
                highpa.mark();
                LOGGER.error("bad_sensor=pa sensor_val={} device_id={} fw_version={}", pa, device_id, fw_version);
            }

            if (uv > HIGH_UV_THRESHOLD) {
                highuv.mark();
                LOGGER.error("bad_sensor=uv sensor_val={} device_id={} fw_version={}", uv, device_id, fw_version);
            }

            if (dust > HIGH_DUST_THRESHOLD) {
                highdust.mark();
                LOGGER.error("bad_sensor=uv sensor_val={} device_id={} fw_version={}", uv, device_id, fw_version);
            }

            if (uvlux > HIGH_LUX_THRESHOLD) {
                highlux.mark();
                LOGGER.error("bad_sensor=uvlux sensor_val={} device_id={} fw_version={}", uvlux, device_id, fw_version);
            }

            if (clearlux > HIGH_LUX_THRESHOLD) {
                highlux.mark();
                LOGGER.error("bad_sensor=clearlux sensor_val={} device_id={} fw_version={}", clearlux, device_id, fw_version);
            }

            if (hum < LOW_HUMIDITY_THRESHOLD) {
                lowhum.mark();
                LOGGER.error("bad_sensor=hum sensor_val={} device_id={} fw_version={}", hum, device_id, fw_version);

            } else if (hum > HIGH_HUMIDITY_THRESHOLD) {
                highhum.mark();
                LOGGER.error("bad_sensor=hum sensor_val={} device_id={} fw_version={}", hum, device_id, fw_version);
            }

            if (tmp < LOW_TEMP_THRESHOLD) {
                lowtmp.mark();
                LOGGER.error("bad_sensor=tmp sensor_val={} device_id={} fw_version={}", tmp, device_id, fw_version);

            } else if (tmp > HIGH_TEMP_THRESHOLD) {
                hightmp.mark();
                LOGGER.error("bad_sensor=tmp sensor_val={} device_id={} fw_version={}", tmp, device_id, fw_version);
            }

            if (db > HIGH_NOISE_THRESHOLD) {
                highdb.mark();
                LOGGER.error("bad_sensor=db sensor_val={} device_id={} fw_version={}", db, device_id, fw_version);

            }


        }
    }

}
