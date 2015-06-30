package com.hello.suripu.analytics.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.hello.suripu.analytics.models.FirmwareInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Map;

public class ActiveDevicesTracker {
    public static final String SENSE_ACTIVE_SET_KEY = "active_senses";
    public static final String PILL_ACTIVE_SET_KEY = "active_pills";
    public static final String FIRMWARES_SEEN_SET_KEY = "firmwares_seen";
    public static final String ALL_DEVICES_SEEN_SET_KEY = "all_seen_senses";

    private final static Logger LOGGER = LoggerFactory.getLogger(ActiveDevicesTracker.class);
    private final JedisPool jedisPool;

    public ActiveDevicesTracker(final JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public void trackSense(final String senseId, final Long lastSeen) {
        final Map<String, Long> seenDevices = Maps.newHashMap();
        seenDevices.put(senseId, lastSeen);
        trackSenses(ImmutableMap.copyOf(seenDevices));
    }

    public void trackSenses(final Map<String, Long> activeSenses) {
        trackDevices(SENSE_ACTIVE_SET_KEY, ImmutableMap.copyOf(activeSenses));
    }

    public void trackAllSeenSenses(final Map<String, Long> allActiveSenses) {
        trackDevices(ALL_DEVICES_SEEN_SET_KEY, ImmutableMap.copyOf(allActiveSenses));
    }

    public void trackPill(final String pillId, final Long lastSeen) {
        final Map<String, Long> seenDevices = Maps.newHashMap();
        seenDevices.put(pillId, lastSeen);
        trackPills(ImmutableMap.copyOf(seenDevices));
    }

    public void trackPills(final Map<String, Long> activePills) {
        trackDevices(PILL_ACTIVE_SET_KEY, ImmutableMap.copyOf(activePills));
    }

    private void trackDevices(final String redisKey, final Map<String, Long> devicesSeen) {
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();

            final Pipeline pipe = jedis.pipelined();
            pipe.multi();
            for(Map.Entry<String, Long> entry : devicesSeen.entrySet()) {
                pipe.zadd(redisKey, entry.getValue(), entry.getKey());
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
}
