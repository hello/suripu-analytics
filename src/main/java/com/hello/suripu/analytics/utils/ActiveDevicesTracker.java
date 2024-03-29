package com.hello.suripu.analytics.utils;

import com.google.common.collect.ImmutableMap;

import com.hello.suripu.analytics.models.WifiInfo;
import com.hello.suripu.core.models.FirmwareInfo;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

public class ActiveDevicesTracker {
    private static final String SENSE_ACTIVE_SET_KEY = "active_senses";
    private static final String PILL_ACTIVE_SET_KEY = "active_pills";
    private static final String DEVICE_ID_KEY_BASE = "device_id:";
    private static final String TOP_FIRMWARE_KEY_BASE = "top:";
    private static final String MIDDLE_FIRMWARE_KEY_BASE = "middle:";
    private static final String FIRMWARES_SEEN_SET_KEY = "firmwares_seen";
    private static final String TOP_FIRMWARES_SEEN_SET_KEY = "top_firmwares_seen";
    private static final String WIFI_INFO_HASH_KEY = "wifi_info";
    private static final String SENSE_UPTIME_HSET_KEY = "sense_uptimes";
    private static final String HOURLY_ACTIVE_SENSE_SET_KEY_PREFIX = "hourly_active_sense_%s";
    private static final String HOURLY_ACTIVE_PILL_SET_KEY_PREFIX = "hourly_active_pill_%s";
    private static final DateTimeFormatter SET_KEY_SUFFIX_PATTERN = DateTimeFormat.forPattern("yyyy_MM_dd_HH_00");
    private static final Integer HOURLY_SET_KEY_EXPIRATION_IN_HOURS = 48;

    private static final String GENERIC_EXCEPTION_LOG_MESSAGE = "error=jedis-connection-exception";
    private final static Logger LOGGER = LoggerFactory.getLogger(ActiveDevicesTracker.class);

    private final JedisPool jedisPool;

    public ActiveDevicesTracker(final JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public void trackSenses(final Map<String, Long> activeSenses) {
        trackDevices(SENSE_ACTIVE_SET_KEY, HOURLY_ACTIVE_SENSE_SET_KEY_PREFIX, ImmutableMap.copyOf(activeSenses));
    }

    public void trackPills(final Map<String, Long> activePills) {
        trackDevices(PILL_ACTIVE_SET_KEY, HOURLY_ACTIVE_PILL_SET_KEY_PREFIX, ImmutableMap.copyOf(activePills));
    }

    private void trackDevices(final String activeKey, final String hourlyActiveKeySetPrefix, final Map<String, Long> devicesSeen) {
        final DateTime dateTimeNow = DateTime.now(DateTimeZone.UTC);
        final String hourlyActiveSetKey = String.format(
                hourlyActiveKeySetPrefix,
                dateTimeNow.toString(SET_KEY_SUFFIX_PATTERN)
        );

        final Long hourlyActiveKeySetExpirationTimestampSeconds = dateTimeNow
                .minusMillis(dateTimeNow.getMillisOfSecond())
                .minusSeconds(dateTimeNow.getSecondOfMinute())
                .minusMinutes(dateTimeNow.getMinuteOfHour())
                .plusHours(HOURLY_SET_KEY_EXPIRATION_IN_HOURS)
                .getMillis() / 1000;
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();

            final Pipeline pipe = jedis.pipelined();
            pipe.multi();
            for(Map.Entry<String, Long> entry : devicesSeen.entrySet()) {
                pipe.zadd(activeKey, entry.getValue(), entry.getKey());
                pipe.sadd(hourlyActiveSetKey, entry.getKey());
                pipe.expireAt(hourlyActiveSetKey, hourlyActiveKeySetExpirationTimestampSeconds);
            }
            pipe.exec();
        }catch (JedisDataException exception) {
            LOGGER.error("error=jedis-data-exception message={}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        } catch(Exception exception) {
            LOGGER.error("error=redis-unknown-failure message={}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        }
        finally {
            try{
                jedisPool.returnResource(jedis);
            }catch (JedisConnectionException e) {
                LOGGER.error(GENERIC_EXCEPTION_LOG_MESSAGE + " message={}", e.getMessage());
            }
        }
        LOGGER.debug("action=active-devices-tracked device_count={} key={}", devicesSeen.size(), activeKey);
    }

    public void trackFirmwares(final Map<String, FirmwareInfo> seenFirmwares) {
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            final Pipeline pipe = jedis.pipelined();
            pipe.multi();
            for(final Map.Entry <String, FirmwareInfo> entry : seenFirmwares.entrySet()) {
                final FirmwareInfo fwEntry = entry.getValue();

                if (!fwEntry.middleVersion.equals("0")) {
                    pipe.zadd(FIRMWARES_SEEN_SET_KEY, fwEntry.timestamp, fwEntry.middleVersion);
                    pipe.hset(DEVICE_ID_KEY_BASE.concat(fwEntry.device_id), "middle_version", fwEntry.middleVersion);
                    pipe.hset(DEVICE_ID_KEY_BASE.concat(fwEntry.device_id), "timestamp", fwEntry.timestamp.toString());
                    pipe.zadd(MIDDLE_FIRMWARE_KEY_BASE.concat(fwEntry.middleVersion), fwEntry.timestamp, fwEntry.device_id);
                }
                if (!fwEntry.topVersion.equals("0")) {
                    pipe.zadd(TOP_FIRMWARES_SEEN_SET_KEY, fwEntry.timestamp, fwEntry.topVersion);
                    pipe.hset(DEVICE_ID_KEY_BASE.concat(fwEntry.device_id), "top_version", fwEntry.topVersion);
                    pipe.hset(DEVICE_ID_KEY_BASE.concat(fwEntry.device_id), "timestamp", fwEntry.timestamp.toString());
                    pipe.zadd(TOP_FIRMWARE_KEY_BASE.concat(fwEntry.topVersion), fwEntry.timestamp, fwEntry.device_id);
                }
            }
            pipe.exec();
        }catch (JedisDataException exception) {
            LOGGER.error("error=jedis-data-exception message={}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        } catch(Exception exception) {
            LOGGER.error("error=redis-unknown-failure message={}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        }
        finally {
            try{
                jedisPool.returnResource(jedis);
            }catch (JedisConnectionException e) {
                LOGGER.error(GENERIC_EXCEPTION_LOG_MESSAGE + " message={}", e.getMessage());
            }
        }
        LOGGER.debug("action=firmware-info-tracked device_count={}", seenFirmwares.size());
    }

    public void trackWifiInfo(final Map<String, WifiInfo> wifiInfos) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            final Pipeline pipe = jedis.pipelined();
            pipe.multi();
            for(final Map.Entry <String, WifiInfo> entry : wifiInfos.entrySet()) {
                final WifiInfo wifiInfo = entry.getValue();
                pipe.hset(WIFI_INFO_HASH_KEY, entry.getKey(), String.format("%s : %s", wifiInfo.ssid, wifiInfo.rssi));
            }
            pipe.exec();
        }catch (JedisDataException exception) {
            LOGGER.error("error=jedis-data-exception message={}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        } catch(Exception exception) {
            LOGGER.error("error=redis-unknown-failure message={}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        }
        finally {
            try{
                jedisPool.returnResource(jedis);
            }catch (JedisConnectionException e) {
                LOGGER.error(GENERIC_EXCEPTION_LOG_MESSAGE + " message={}", e.getMessage());
            }
        }
        LOGGER.debug("action=wifi-info-tracked device_count={}", wifiInfos.size());
    }

    public void trackUptime(final Map<String, Integer> uptimes) {
        trackUptime(SENSE_UPTIME_HSET_KEY, ImmutableMap.copyOf(uptimes));
    }

    private void trackUptime(final String redisKey, final Map<String, Integer> uptime) {
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            final Pipeline pipe = jedis.pipelined();
            pipe.multi();
            for(final Map.Entry <String, Integer> entry : uptime.entrySet()) {
                pipe.hset(redisKey, entry.getKey(), String.valueOf(entry.getValue()));
            }
            pipe.exec();
        }catch (JedisDataException exception) {
            LOGGER.error("error=jedis-data-exception message={}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        } catch(Exception exception) {
            LOGGER.error("error=redis-unknown-failure message={}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        }
        finally {
            try{
                jedisPool.returnResource(jedis);
            }catch (JedisConnectionException e) {
                LOGGER.error(GENERIC_EXCEPTION_LOG_MESSAGE + " message={}", e.getMessage());
            }
        }
        LOGGER.debug("action=track-uptime num_senses={}", uptime.size());
    }
}
