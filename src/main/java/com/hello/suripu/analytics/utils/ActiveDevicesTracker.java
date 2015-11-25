package com.hello.suripu.analytics.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.hello.suripu.analytics.models.WifiInfo;
import com.hello.suripu.core.models.FirmwareInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

public class ActiveDevicesTracker {
    public static final String SENSE_ACTIVE_SET_KEY = "active_senses";
    public static final String PILL_ACTIVE_SET_KEY = "active_pills";
    public static final String FIRMWARES_SEEN_SET_KEY = "firmwares_seen";
    public static final String ALL_DEVICES_SEEN_SET_KEY = "all_seen_senses";
    public static final String WIFI_INFO_HASH_KEY = "wifi_info";
    public static final String SENSE_SNAPSHOT_SET_KEY = "seen_sense_snapshot";
    public static final Integer SECONDS_IN_48_HOURS = 172800;

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

    //Store a snapshot of seen senses in a rolling set of keys
    public void snapshotSeenSenses() {
      Jedis jedis = null;


      final SimpleDateFormat dateFormat = new SimpleDateFormat("_ddHH");
      dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
      final String snapshotKeyName = SENSE_SNAPSHOT_SET_KEY + dateFormat.format(new Date());

      try {
        jedis = jedisPool.getResource();

        final Pipeline pipe = jedis.pipelined();
        pipe.multi();
          pipe.zunionstore(snapshotKeyName, SENSE_ACTIVE_SET_KEY);
          pipe.expire(snapshotKeyName, SECONDS_IN_48_HOURS);
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
      LOGGER.debug("Snapshot of seen senses stored to key {}", snapshotKeyName);
    }
}
