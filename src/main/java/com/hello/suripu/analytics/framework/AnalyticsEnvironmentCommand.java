package com.hello.suripu.analytics.framework;

import io.dropwizard.cli.ConfiguredCommand;

import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * A command which executes with a configured {@link Environment}.
 *
 * @param <T> the {@link Configuration} subclass which is loaded from the configuration file
 * @see Configuration
 */
public abstract class AnalyticsEnvironmentCommand<T extends Configuration> extends ConfiguredCommand<T> {

    /**
     * Creates a new environment command.
     *
     * @param name        the name of the command, used for command line invocation
     * @param description a description of the command's purpose
     */
    protected AnalyticsEnvironmentCommand(String name, String description) {
        super(name, description);
    }

    @Override
    protected final void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        final Environment environment = new Environment(bootstrap.getApplication().getName(),
                bootstrap.getObjectMapper(),
                bootstrap.getValidatorFactory().getValidator(),
                bootstrap.getMetricRegistry(),
                bootstrap.getClassLoader());
        configuration.getMetricsFactory().configure(environment.lifecycle(),
                bootstrap.getMetricRegistry());
        bootstrap.run(configuration, environment);
        run(environment, namespace, configuration);
    }

    /**
     * Runs the command with the given {@link Environment} and {@link Configuration}.
     *
     * @param environment   the configured environment
     * @param namespace     the parsed command line namespace
     * @param configuration the configuration object
     * @throws Exception if something goes wrong
     */
    protected abstract void run(Environment environment, Namespace namespace, T configuration) throws Exception;
}