package com.hello.suripu.analytics.sense;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.hello.suripu.analytics.configuration.AnalyticsConfiguration;
import com.hello.suripu.analytics.framework.AnalyticsEnvironmentCommand;
import io.dropwizard.setup.Environment;
import java.net.InetAddress;
import net.sourceforge.argparse4j.inf.Namespace;
import redis.clients.jedis.JedisPool;

/**
 * Created by jnorgan on 6/29/15.
 */
public class SenseSaveAnalyticsCommand extends AnalyticsEnvironmentCommand<AnalyticsConfiguration> {

    private final static String kinesisStreamName = "sense_sensors_data";

    public SenseSaveAnalyticsCommand(final String name, final String description) {
        super(name, description);
    }

    @Override
    protected void run(Environment environment, Namespace namespace, AnalyticsConfiguration configuration) throws Exception {

        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        final String workerId = InetAddress.getLocalHost().getCanonicalHostName();

        final KinesisClientLibConfiguration kinesisConfig = new KinesisClientLibConfiguration(
                configuration.getAppName(),
                kinesisStreamName,
                awsCredentialsProvider,
                workerId);
        kinesisConfig.withMaxRecords(configuration.getMaxRecords());
        kinesisConfig.withKinesisEndpoint(configuration.getKinesisEndpoint());

        final JedisPool jedisPool = new JedisPool(
                configuration.getRedisConfiguration().getHost(),
                configuration.getRedisConfiguration().getPort()
        );

        final IRecordProcessorFactory processorFactory = new SenseSaveProcessorFactory();

        final Worker kinesisWorker = new Worker(processorFactory, kinesisConfig);
        kinesisWorker.run();
    }
}
