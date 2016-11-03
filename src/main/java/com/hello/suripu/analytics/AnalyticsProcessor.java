package com.hello.suripu.analytics;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.collect.ImmutableList;
import com.hello.suripu.analytics.cli.CreateDynamoDBTables;
import com.hello.suripu.analytics.configuration.AnalyticsConfiguration;
import com.hello.suripu.analytics.processors.KinesisWorkerManager;
import com.hello.suripu.analytics.processors.PillStatsProcessorFactory;
import com.hello.suripu.analytics.processors.SenseStatsCommand;
import com.hello.suripu.analytics.processors.SenseStatsProcessorFactory;
import com.hello.suripu.coredropwizard.metrics.RegexMetricFilter;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A dropwizard application that essentially mimics the same/similar structure as the
 * Suripu workers.
 *
 */
public class AnalyticsProcessor extends Application<AnalyticsConfiguration>
{

    private final static Logger LOGGER = LoggerFactory.getLogger(AnalyticsProcessor.class);

    private final static String PILL_COMMAND_APP_NAME = "pill_stats";
    private final static String PILL_COMMAND_STREAM_NAME = "batch_pill_data";
    private final static String SENSE_COMMAND_APP_NAME = "sense_stats";
    private final static String SENSE_COMMAND_STREAM_NAME = "sense_sensors_data";

    private final static String CHECKPOINT_TABLE_NAME = "kinesis_checkpoint_track";
    private final static ClientConfiguration DEFAULT_CLIENT_CONFIGURATION = new ClientConfiguration()
            .withConnectionTimeout(200)
            .withMaxErrorRetry(1)
            .withMaxConnections(10);

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
//        bootstrap.addCommand(new PillStatsCommand("pill_stats", "Analyzing incoming pill data."));
        bootstrap.addCommand(new CreateDynamoDBTables());
    }

    @Override
    public void run(AnalyticsConfiguration configuration,
                    Environment environment) throws UnknownHostException {

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
        final KinesisClientLibConfiguration pillKinesisConfig = new KinesisClientLibConfiguration(
                configuration.getAppNames().get(PILL_COMMAND_APP_NAME),
                configuration.getKinesisStreams().get(PILL_COMMAND_STREAM_NAME),
                awsCredentialsProvider,
                workerId);
        pillKinesisConfig.withInitialPositionInStream(InitialPositionInStream.LATEST);
        pillKinesisConfig.withMaxRecords(configuration.getMaxRecords());
        pillKinesisConfig.withKinesisEndpoint(configuration.getKinesisEndpoints().get(PILL_COMMAND_STREAM_NAME));
        pillKinesisConfig.withIdleTimeBetweenReadsInMillis(10000);

        final JedisPool jedisPool = new JedisPool(
                configuration.getRedisConfiguration().getHost(),
                configuration.getRedisConfiguration().getPort()
        );

        final IRecordProcessorFactory pillProcessorFactory = new PillStatsProcessorFactory(
                jedisPool,
                checkpoinTrackerclient,
                configuration.getKinesisStreams().get(PILL_COMMAND_STREAM_NAME),
                configuration.dynamoDBConfiguration().tables().get(CHECKPOINT_TABLE_NAME),
                environment.metrics()
        );

        final Worker pillWorker = new Worker(pillProcessorFactory , pillKinesisConfig);
        final ExecutorService executorService = environment.lifecycle()
                .executorService("analytics")
                .workQueue(new LinkedBlockingQueue(configuration.maxThreads()))
                .minThreads(configuration.minThreads())
                .build();

        // SENSE

        final KinesisClientLibConfiguration kinesisConfig = new KinesisClientLibConfiguration(
                configuration.getAppNames().get(SENSE_COMMAND_APP_NAME),
                configuration.getKinesisStreams().get(SENSE_COMMAND_STREAM_NAME),
                awsCredentialsProvider,
                workerId);
        kinesisConfig.withInitialPositionInStream(InitialPositionInStream.LATEST);
        kinesisConfig.withMaxRecords(configuration.getMaxRecords());
        kinesisConfig.withKinesisEndpoint(configuration.getKinesisEndpoints().get(SENSE_COMMAND_STREAM_NAME));
        kinesisConfig.withIdleTimeBetweenReadsInMillis(10000);

        final IRecordProcessorFactory senseProcessorFactory = new SenseStatsProcessorFactory(
                jedisPool,
                checkpoinTrackerclient,
                configuration.getKinesisStreams().get(SENSE_COMMAND_STREAM_NAME),
                configuration.dynamoDBConfiguration().tables().get(CHECKPOINT_TABLE_NAME),
                environment.metrics()
        );

        final Worker senseWorker = new Worker(senseProcessorFactory, kinesisConfig);


        // Wiring
        environment.lifecycle().manage(new KinesisWorkerManager(executorService, pillWorker));
        environment.lifecycle().manage(new KinesisWorkerManager(executorService, senseWorker));
        environment.jersey().register(new ConfigurationResource(configuration));
    }
}
