package com.hello.suripu.analytics.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.hello.suripu.core.configuration.QueueName;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class KinesisConfiguration {

    @Valid
    @NotNull
    @JsonProperty
    private String endpoint;

    public String getEndpoint() {
        return endpoint;
    }


    @Valid
    @NotNull
    @JsonProperty("streams")
    private Map<String, String> streams;

    public ImmutableMap<String, String> getStreams() {
        return ImmutableMap.copyOf(streams);
    }
}
