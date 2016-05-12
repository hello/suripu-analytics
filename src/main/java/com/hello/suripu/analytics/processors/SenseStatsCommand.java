package com.hello.suripu.analytics.processors;

import com.google.common.collect.ImmutableList;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.hello.suripu.analytics.configuration.AnalyticsConfiguration;
import com.hello.suripu.analytics.framework.AnalyticsEnvironmentCommand;
import com.hello.suripu.coredw8.metrics.RegexMetricFilter;

import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Created by jnorgan on 6/29/15.
 */
public class SenseStatsCommand extends AnalyticsEnvironmentCommand<AnalyticsConfiguration> {
    private final static String COMMAND_APP_NAME = "sense_stats";
    private final static String COMMAND_STREAM_NAME = "sense_sensors_data";
    private final static String CHECKPOINT_TABLE_NAME = "kinesis_checkpoint_track";
    private final static Logger LOGGER = LoggerFactory.getLogger(SenseStatsCommand.class);
    private final static ClientConfiguration DEFAULT_CLIENT_CONFIGURATION = new ClientConfiguration().withConnectionTimeout(200).withMaxErrorRetry(1);

    public SenseStatsCommand(final String name, final String description) {
        super(name, description);
    }

    @Override
    protected void run(Environment environment, Namespace namespace, AnalyticsConfiguration configuration) throws Exception {

        if(configuration.getMetricsEnabled()) {
            final String graphiteHostName = configuration.getGraphite().getHost();
            final String apiKey = configuration.getGraphite().getApiKey();
            final Integer interval = configuration.getGraphite().getReportingIntervalInSeconds();

            final String env = (configuration.getDebug()) ? "dev" : "prod";
            final String prefix = String.format("%s.%s.suripu-analytics", apiKey, env);

            final ImmutableList<String> metrics = ImmutableList.copyOf(configuration.getGraphite().getIncludeMetrics());
            final RegexMetricFilter metricFilter = new RegexMetricFilter(metrics);

            final Graphite graphite = new Graphite(new InetSocketAddress(graphiteHostName, 2003));

            final GraphiteReporter reporter = GraphiteReporter.forRegistry(environment.metrics())
                    .prefixedWith(prefix)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(metricFilter)
                    .build(graphite);
            reporter.start(interval, TimeUnit.SECONDS);

            LOGGER.info("Metrics enabled.");
        } else {
            LOGGER.warn("Metrics not enabled.");
        }

        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();

        final AmazonDynamoDB checkpoinTrackerclient = new AmazonDynamoDBClient(awsCredentialsProvider, DEFAULT_CLIENT_CONFIGURATION);
        checkpoinTrackerclient.setEndpoint(configuration.dynamoDBConfiguration().endpoints().get(CHECKPOINT_TABLE_NAME));

        final String workerId = InetAddress.getLocalHost().getCanonicalHostName();
        final KinesisClientLibConfiguration kinesisConfig = new KinesisClientLibConfiguration(
                configuration.getAppNames().get(COMMAND_APP_NAME),
                configuration.getKinesisStreams().get(COMMAND_STREAM_NAME),
                awsCredentialsProvider,
                workerId);
        kinesisConfig.withInitialPositionInStream(InitialPositionInStream.LATEST);
        kinesisConfig.withMaxRecords(configuration.getMaxRecords());
        kinesisConfig.withKinesisEndpoint(configuration.getKinesisEndpoints().get(COMMAND_STREAM_NAME));
        kinesisConfig.withIdleTimeBetweenReadsInMillis(10000);

        final JedisPool jedisPool = new JedisPool(
                configuration.getRedisConfiguration().getHost(),
                configuration.getRedisConfiguration().getPort()
        );

        final IRecordProcessorFactory processorFactory = new SenseStatsProcessorFactory(
                jedisPool,
                checkpoinTrackerclient,
                configuration.getKinesisStreams().get(COMMAND_STREAM_NAME),
                configuration.dynamoDBConfiguration().tables().get(CHECKPOINT_TABLE_NAME),
                environment.metrics()
        );

        final Worker kinesisWorker = new Worker(processorFactory, kinesisConfig);
        kinesisWorker.run();
    }
}
