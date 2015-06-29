package com.hello.suripu.analytics;

import com.hello.suripu.analytics.configuration.AnalyticsConfiguration;
import com.hello.suripu.analytics.sense.SenseSaveAnalyticsCommand;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.TimeZone;
import org.joda.time.DateTimeZone;

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
        bootstrap.addCommand(new SenseSaveAnalyticsCommand("sense_save", "Analyzing sense save data."));
    }

    @Override
    public void run(AnalyticsConfiguration configuration,
                    Environment environment) {
        // Nothing to see here
    }
}
