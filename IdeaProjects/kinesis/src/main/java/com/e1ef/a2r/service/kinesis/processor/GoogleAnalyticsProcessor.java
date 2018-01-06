package com.e1ef.a2r.service.kinesis.processor;


import java.net.InetAddress;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.e1ef.a2r.service.kinesis.util.ConfigurationUtils;
import com.e1ef.a2r.service.kinesis.util.CredentialUtils;

/**
 * Uses the Kinesis Client Library (KCL) to continuously consume and process stock trade
 * records from the stock trades stream. KCL monitors the number of shards and creates
 * record processor instances to read and process records from each shard. KCL also
 * load balances shards across all the instances of this processor.
 *
 */
public class GoogleAnalyticsProcessor {

    private static final Log LOG = LogFactory.getLog(GoogleAnalyticsProcessor.class);

    private static final Logger ROOT_LOGGER = Logger.getLogger("");
    private static final Logger PROCESSOR_LOGGER =
            Logger.getLogger("com.e1ef.a2r.service.kinesis.processor");
    private static final InitialPositionInStream APPLICATION_INITIAL_POSITION_IN_STREAM =
            InitialPositionInStream.TRIM_HORIZON;

    private static void checkUsage(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: " + GoogleAnalyticsProcessor.class.getSimpleName()
                    + " <application name> <stream name> <region>");
            System.exit(1);
        }
    }

    /**
     * Sets the global log level to WARNING and the log level for this package to INFO,
     * so that we only see INFO messages for this processor. This is just for the purpose
     * of this tutorial, and should not be considered as best practice.
     */
    private static void setLogLevels() {
        ROOT_LOGGER.setLevel(Level.ALL);
        PROCESSOR_LOGGER.setLevel(Level.ALL);
    }

    public static void main(String[] args) throws Exception {
        checkUsage(args);

        String applicationName = args[0];
        String streamName = args[1];
        Region region = RegionUtils.getRegion(args[2]);
        if (region == null) {
            System.err.println(args[2] + " is not a valid AWS region.");
            System.exit(1);
        }

        setLogLevels();

        AWSCredentialsProvider credentialsProvider = CredentialUtils.getCredentialsProvider();

        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        LOG.info(workerId);
        KinesisClientLibConfiguration kclConfig =
                new KinesisClientLibConfiguration(applicationName, streamName, credentialsProvider, workerId)
                        .withRegionName(region.getName())
                        .withCommonClientConfig(ConfigurationUtils.getClientConfigWithUserAgent())
                        .withIdleTimeBetweenReadsInMillis(250)
                        .withInitialPositionInStream(APPLICATION_INITIAL_POSITION_IN_STREAM);

        IRecordProcessorFactory recordProcessorFactory = new GoogleAnalyticsProcessorFactory();


        final Worker worker = new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(kclConfig)
                .build();

        int exitCode = 0;
        try {
            worker.run();

        } catch (Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }
        System.exit(exitCode);

    }
}
