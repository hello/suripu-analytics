package com.hello.suripu.analytics.models;


public class WifiInfo {
    public final Integer rssi;
    public final String ssid;

    public WifiInfo(final Integer rssi, final String ssid) {
        this.rssi = rssi;
        this.ssid = ssid;
    }
}
