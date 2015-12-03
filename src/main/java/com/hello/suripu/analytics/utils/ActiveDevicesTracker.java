package com.hello.suripu.analytics.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.hello.suripu.analytics.models.WifiInfo;
import com.hello.suripu.core.models.FirmwareInfo;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Map;
import java.util.Set;

public class ActiveDevicesTracker {
    private static final String SENSE_ACTIVE_SET_KEY = "active_senses";
    private static final String PILL_ACTIVE_SET_KEY = "active_pills";
    private static final String FIRMWARES_SEEN_SET_KEY = "firmwares_seen";
    private static final String WIFI_INFO_HASH_KEY = "wifi_info";
    private static final String HOURLY_ACTIVE_SENSE_SET_KEY_PREFIX = "hourly_active_sense_%s";
    private static final String HOURLY_ACTIVE_PILL_SET_KEY_PREFIX = "hourly_active_pill_%s";
    private static final DateTimeFormatter SET_KEY_SUFFIX_PATTERN = DateTimeFormat.forPattern("yyyy_MM_dd_HH_00");
    private static final Integer HOURLY_SET_KEY_EXPIRATION = 172800;

    private final static Logger LOGGER = LoggerFactory.getLogger(ActiveDevicesTracker.class);

    private final JedisPool jedisPool;
    private final Set<String> createdHourlyActiveSetKeys = Sets.newHashSet();

    public ActiveDevicesTracker(final JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public void trackSenses(final Map<String, Long> activeSenses) {
        final String hourlyActiveSenseSetKey = String.format(
                HOURLY_ACTIVE_SENSE_SET_KEY_PREFIX,
                DateTime.now(DateTimeZone.UTC).toString(SET_KEY_SUFFIX_PATTERN)
        );
        trackDevices(SENSE_ACTIVE_SET_KEY, hourlyActiveSenseSetKey, ImmutableMap.copyOf(activeSenses));
    }


    public void trackPills(final Map<String, Long> activePills) {
        final String hourlyActivePillSetKey = String.format(
                HOURLY_ACTIVE_PILL_SET_KEY_PREFIX,
                DateTime.now(DateTimeZone.UTC).toString(SET_KEY_SUFFIX_PATTERN)
        );
        trackDevices(PILL_ACTIVE_SET_KEY, hourlyActivePillSetKey, ImmutableMap.copyOf(activePills));
    }

    private void trackDevices(final String activeKey, final String hourlyActiveKey, final Map<String, Long> devicesSeen) {
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();

            final Pipeline pipe = jedis.pipelined();
            pipe.multi();
            for(Map.Entry<String, Long> entry : devicesSeen.entrySet()) {
                pipe.zadd(activeKey, entry.getValue(), entry.getKey());
                pipe.sadd(hourlyActiveKey, entry.getKey());
                if(!createdHourlyActiveSetKeys.contains(entry.getKey())) {
                    pipe.expire(hourlyActiveKey, HOURLY_SET_KEY_EXPIRATION);
                    createdHourlyActiveSetKeys.add(entry.getKey());
                }
            }
            pipe.exec();
        }catch (JedisDataException exception) {
            LOGGER.error("Failed getting data out of redis: {}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        } catch(Exception exception) {
            LOGGER.error("Unknown error connection to redis: {}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        }
        finally {
            try{
                jedisPool.returnResource(jedis);
            }catch (JedisConnectionException e) {
                LOGGER.error("Jedis Connection Exception while returning resource to pool. Redis server down?");
            }
        }
        LOGGER.debug("Tracked {} active devices", devicesSeen.size());
    }

    public void trackFirmwares(final Map<String, FirmwareInfo> seenFirmwares) {
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            final Pipeline pipe = jedis.pipelined();
            pipe.multi();
            for(final Map.Entry <String, FirmwareInfo> entry : seenFirmwares.entrySet()) {
                final FirmwareInfo fwEntry = entry.getValue();
                pipe.zadd(FIRMWARES_SEEN_SET_KEY, fwEntry.timestamp, fwEntry.version);
                pipe.zadd(fwEntry.version, fwEntry.timestamp, fwEntry.device_id);
            }
            pipe.exec();
        }catch (JedisDataException exception) {
            LOGGER.error("Failed getting data out of redis: {}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        } catch(Exception exception) {
            LOGGER.error("Unknown error connection to redis: {}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        }
        finally {
            try{
                jedisPool.returnResource(jedis);
            }catch (JedisConnectionException e) {
                LOGGER.error("Jedis Connection Exception while returning resource to pool. Redis server down?");
            }
        }
        LOGGER.debug("Tracked {} device firmware versions", seenFirmwares.size());
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
            LOGGER.error("Failed getting data out of redis: {}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        } catch(Exception exception) {
            LOGGER.error("Unknown error connection to redis: {}", exception.getMessage());
            jedisPool.returnBrokenResource(jedis);
            return;
        }
        finally {
            try{
                jedisPool.returnResource(jedis);
            }catch (JedisConnectionException e) {
                LOGGER.error("Jedis Connection Exception while returning resource to pool. Redis server down?");
            }
        }
        LOGGER.debug("Tracked wifi info for  {} senses", wifiInfos.size());
    }
}
