package com.hello.suripu.analytics;

import com.hello.suripu.analytics.configuration.AnalyticsConfiguration;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/config")
public class ConfigurationResource {

    private final AnalyticsConfiguration configuration;

    public ConfigurationResource(AnalyticsConfiguration configuration) {
        this.configuration = configuration;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public AnalyticsConfiguration config() {
        return configuration;
    }
}
