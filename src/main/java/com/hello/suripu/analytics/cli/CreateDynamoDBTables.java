package com.hello.suripu.analytics.cli;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.hello.suripu.analytics.configuration.AnalyticsConfiguration;
import com.hello.suripu.analytics.configuration.NewDynamoDBConfiguration;
import com.hello.suripu.analytics.utils.CheckpointTracker;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;

public class CreateDynamoDBTables extends ConfiguredCommand<AnalyticsConfiguration> {

    public CreateDynamoDBTables() {
        super("create_dynamodb_tables", "Create dynamoDB tables");
    }

    @Override
    protected void run(Bootstrap<AnalyticsConfiguration> bootstrap, Namespace namespace, AnalyticsConfiguration configuration) throws Exception {
        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();

        createCheckpointTrackingTable(configuration, awsCredentialsProvider);
    }

    private void createCheckpointTrackingTable(final AnalyticsConfiguration configuration, final AWSCredentialsProvider awsCredentialsProvider) {
        final NewDynamoDBConfiguration config = configuration.dynamoDBConfiguration();
        final AmazonDynamoDBClient client = new AmazonDynamoDBClient(awsCredentialsProvider);

        //TODO: Add checkpoint tracking to DynamoDBTableName in core
        client.setEndpoint(config.endpoints().get("kinesis_checkpoint_track"));
        final String tableName = config.tables().get("kinesis_checkpoint_track");
        try {
            client.describeTable(tableName);
            System.out.println(String.format("%s already exists.", tableName));
        } catch (AmazonServiceException exception) {
            final CreateTableResult result = CheckpointTracker.createTable(tableName, client);
            final TableDescription description = result.getTableDescription();
            System.out.println(tableName + " " + description.getTableStatus());
        }
    }
}
