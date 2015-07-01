package com.hello.suripu.analytics.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class NewDynamoDBConfiguration {
    @Valid
    @NotNull
    @JsonProperty
    private String region = "us-east-1";

    public String getRegion() {
        return region;
    }


    @Valid
    @NotNull
    @JsonProperty("tables")
    private Map<String, String> tables = Maps.newHashMap();

    public ImmutableMap<String, String> tables() {
        return ImmutableMap.copyOf(tables);
    }

    @Valid
    @NotNull
    @JsonProperty("endpoints")
    private Map<String, String> endpoints = Maps.newHashMap();

    public ImmutableMap<String, String> endpoints() {
        return ImmutableMap.copyOf(endpoints);
    }

}
