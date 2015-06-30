package com.hello.suripu.analytics.models;

/**
 * Created by jnorgan on 6/29/15.
 */
public class FirmwareInfo {
    public final String version;
    public final String device_id;
    public final Long timestamp;


    public FirmwareInfo(final String version, final String device_id, final Long timestamp) {
        this.version = version;
        this.device_id = device_id;
        this.timestamp = timestamp;
    }
}