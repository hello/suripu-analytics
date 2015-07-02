package com.hello.suripu.analytics.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class KinesisConfiguration {

    @Valid
    @NotNull
    @JsonProperty("endpoints")
    private Map<String, String> endpoints;

    public ImmutableMap<String, String> getEndpoints() {
        return ImmutableMap.copyOf(endpoints);
    }


    @Valid
    @NotNull
    @JsonProperty("streams")
    private Map<String, String> streams;

    public ImmutableMap<String, String> getStreams() {
        return ImmutableMap.copyOf(streams);
    }
}
