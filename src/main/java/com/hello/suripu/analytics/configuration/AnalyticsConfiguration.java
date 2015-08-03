package com.hello.suripu.analytics.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.collect.ImmutableMap;
import com.hello.suripu.coredw.configuration.GraphiteConfiguration;
import com.hello.suripu.coredw.configuration.RedisConfiguration;
import io.dropwizard.Configuration;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.NotNull;

/**
 * Created by jnorgan on 6/29/15.
 */
public class AnalyticsConfiguration extends Configuration {
    @Valid
    @NotNull
    @JsonProperty("metrics_enabled")
    private Boolean metricsEnabled;

    public Boolean getMetricsEnabled() {
        return metricsEnabled;
    }

    @Valid
    @NotNull
    @JsonProperty("app_names")
    private Map<String, String> appNames;

    public ImmutableMap<String, String> getAppNames() {
        return ImmutableMap.copyOf(appNames);
    }

    @Valid
    @JsonProperty("debug")
    private Boolean debug = Boolean.FALSE;

    public Boolean getDebug() { return debug; }


    @Valid
    @NotNull
    @JsonProperty("kinesis")
    private KinesisConfiguration kinesisConfiguration;

    public Map<String, String> getKinesisEndpoints() {
        return kinesisConfiguration.getEndpoints();
    }

    public Map<String, String> getKinesisStreams() {
        return kinesisConfiguration.getStreams();
    }


    @Valid
    @NotNull
    @JsonProperty("dynamodb")
    private NewDynamoDBConfiguration dynamoDBConfiguration;

    public NewDynamoDBConfiguration dynamoDBConfiguration(){
        return dynamoDBConfiguration;
    }

    @Valid
    @NotNull
    @Max(1000)
    @JsonProperty("max_records")
    private Integer maxRecords;

    public Integer getMaxRecords() {
        return maxRecords;
    }

    @Valid
    @NotNull
    @JsonProperty("redis")
    private RedisConfiguration redisConfiguration;
    public RedisConfiguration getRedisConfiguration() {
        return redisConfiguration;
    }

    @Valid
    @NotNull
    @JsonProperty("graphite")
    private GraphiteConfiguration graphite;

    public GraphiteConfiguration getGraphite() {
        return graphite;
    }
}
