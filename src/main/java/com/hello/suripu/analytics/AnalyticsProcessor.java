package com.hello.suripu.analytics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.hello.suripu.analytics.cli.CreateDynamoDBTables;
import com.hello.suripu.analytics.configuration.AnalyticsConfiguration;
import com.hello.suripu.analytics.processors.PillStatsCommand;
import com.hello.suripu.analytics.processors.SenseStatsCommand;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.joda.time.DateTimeZone;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.TimeZone;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * A dropwizard application that essentially mimics the same/similar structure as the
 * Suripu workers.
 *
 */
public class AnalyticsProcessor extends Application<AnalyticsConfiguration>
{

    public static void main( String[] args ) throws Exception
    {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        DateTimeZone.setDefault(DateTimeZone.UTC);
        new AnalyticsProcessor().run(args);
    }

    @Override
    public String getName() {
        return "suripu-analytics";
    }

    @Override
    public void initialize(Bootstrap<AnalyticsConfiguration> bootstrap) {
        bootstrap.addCommand(new SenseStatsCommand("sense_stats", "Analyzing incoming sense data."));
        bootstrap.addCommand(new PillStatsCommand("pill_stats", "Analyzing incoming pill data."));
        bootstrap.addCommand(new CreateDynamoDBTables());
    }

    @Path("/")
    public static class TestResource {
        private final MetricRegistry metrics;
        private final Meter meter;

        public TestResource(final MetricRegistry metricRegistry) {
            metrics = metricRegistry;
            meter = metrics.meter(name(TestResource.class, "whatever"));
        }



        @GET
        @Timed
        public String something() {
            meter.mark(1);
            return "something";
        }
    }

    @Override
    public void run(AnalyticsConfiguration configuration,
                    Environment environment) {
        environment.jersey().register(new TestResource(environment.metrics()));
    }
}
