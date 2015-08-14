package com.hello.suripu.analytics.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

public class RedisConfiguration extends Configuration {
    @JsonProperty("host")
    private String host;

    public String getHost() {
        return host;
    }

    @JsonProperty("port")
    private Integer port;

    public Integer getPort() {
        return port;
    }
}
