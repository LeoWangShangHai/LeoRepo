package com.e1ef.a2r.service.kinesis.writer;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.e1ef.a2r.service.kinesis.util.CredentialUtils;
import com.e1ef.a2r.service.kinesis.util.LoadProp;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.e1ef.a2r.service.kinesis.util.ConfigurationUtils;


/**
 * Continuously sends simulated stock trades to Kinesis
 *
 */
public class GoogleAnalyticsWriter {

    private static final Log LOG = LogFactory.getLog(GoogleAnalyticsWriter.class);
    private static final Logger ROOT_LOGGER = Logger.getLogger("");
    private static final Logger PROCESSOR_LOGGER =
            Logger.getLogger("com.e1ef.a2r.service.kinesis.writer");

    private static void setLogLevels() {
        ROOT_LOGGER.setLevel(Level.ALL);
        PROCESSOR_LOGGER.setLevel(Level.ALL);

    }
    private static void checkUsage(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: " + GoogleAnalyticsWriter.class.getSimpleName()
                    + " <stream name> <region>");
            System.exit(1);
        }
    }

    private static void validateStream(AmazonKinesis kinesisClient, String streamName) {
        try {
            DescribeStreamResult result = kinesisClient.describeStream(streamName);
            if(!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
                System.err.println("Stream " + streamName + " is not active. Please wait a few moments and try again.");
                System.exit(1);
            }
        } catch (ResourceNotFoundException e) {
            System.err.println("Stream " + streamName + " does not exist. Please create it in the console.");
            System.err.println(e);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error found while describing the stream " + streamName);
            System.err.println(e);
            System.exit(1);
        }
    }

    private static void sendGoogleAnalytics(AmazonKinesis kinesisClient,
                                       String streamName) throws Exception {
        //File file = new File();
        LoadProp loadprop = new LoadProp();
        String datafilename = loadprop.getProperty("data_file");
        String shakehands_filename = loadprop.getProperty("shakehands_file");
        File shakehands_file=new File(shakehands_filename);
        BufferedReader reader = null;
        byte[] byteArray = null;
        File datafile = null;
        String singleLine = null;
        Date date = new Date();
        System.out.println(date.toString());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf1 = new SimpleDateFormat("HH");
        System.out.println(sdf.format(date));
        System.out.println(sdf1.format(date));
        Date currentDate = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
        System.out.println(sdf1.format(currentDate));
        //if (shakehands_file.exists()) {
            try {
                datafile = new File(datafilename);
                reader = new BufferedReader(new InputStreamReader(new FileInputStream(datafile), "UTF8"));
                LOG.info("Sending record to Amazon Kinesis:");
                int count=0;
                //PutRecordRequest putRecord = new PutRecordRequest();
                PutRecordsRequest putRecords = new PutRecordsRequest();
                putRecords.setStreamName(streamName);
                List<PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>();
                String[] result=null;
                while ((singleLine = reader.readLine()) != null) {
                    long createTime = System.currentTimeMillis();
                    singleLine = singleLine + "\n";
                    result = singleLine.split("\001");

                   /* for (int x=0; x<result.length; x++) {
                        //out.print(result[x]);
                        //System.out.print(",");
                    }*/
                    //System.out.println();
                    byteArray = singleLine.getBytes("UTF-8");

                    if (byteArray == null) {
                        LOG.warn("Could not get bytes for Google Analytics data");
                        return;
                    }
                    PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
                    putRecordsRequestEntry.setData(ByteBuffer.wrap(byteArray));
                    putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", count));
                    putRecordsRequestEntryList.add(putRecordsRequestEntry);
                    if(putRecordsRequestEntryList.size()==500){
                        putRecords.setRecords(putRecordsRequestEntryList);
                        PutRecordsResult putRecordsResult  = kinesisClient.putRecords(putRecords);
                        Thread.sleep(500);

                        while (putRecordsResult.getFailedRecordCount() > 0) {
                            LOG.info("Start resending "+putRecordsResult.getFailedRecordCount()+" failed records");
                            final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
                            final List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();
                            for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
                                final PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList.get(i);
                                final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
                                if (putRecordsResultEntry.getErrorCode() != null) {
                                    failedRecordsList.add(putRecordRequestEntry);
                                }
                            }
                            putRecordsRequestEntryList = failedRecordsList;
                            putRecords.setRecords(putRecordsRequestEntryList);
                            Thread.sleep(500);
                            putRecordsResult = kinesisClient.putRecords(putRecords);

                        }
                        putRecordsRequestEntryList.clear();
                    }

                    //putRecords.setRecords(putRecordsRequestEntryList);
                    //
                    //System.out.println("Put Result" + putRecordsResult);


                    //putRecord.setPartitionKey(String.format("partitionKey-%d", createTime));
                    //putRecord.setData(ByteBuffer.wrap(byteArray));
                    //kinesisClient.putRecord(putRecord);
                    //LOG.info(singleLine);
                    count++;
                    if(count%10000==0){
                        LOG.info(count);
                    }
                }
                LOG.info("Sent data file completed, total "+count+" rows");
                reader.close();
                if(shakehands_file.exists())
                {
                    shakehands_file.delete();
                    LOG.info("Shake_hands file deleted successfully");
                }
                else
                {
                    LOG.info("Shake_hands file does not exist");
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (AmazonClientException ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                    }
                }
            }
        //}
       // else{
        //    LOG.warn("Shake hands file is not ready");
        //}
    }

    private static void sendGoogleAnalyticsS3data(AmazonS3 s3Client, AmazonKinesis kinesisClient,
                                            String streamName) throws Exception {

        LoadProp loadprop = new LoadProp();
        String aws_s3_bucket = loadprop.getProperty("aws_s3_bucket");
        String aws_s3_data_file_key = loadprop.getProperty("aws_s3_data_file_key");
        String aws_s3_shake_hands_file_key = loadprop.getProperty("aws_s3_shake_hands_file_key");
        String shake_hands_flag = loadprop.getProperty("shake_hands_flag");

        String datafilename = loadprop.getProperty("data_file");
        String shakehands_filename = loadprop.getProperty("shakehands_file");
        File shakehands_file=new File(shakehands_filename);
        BufferedReader reader = null;
        byte[] byteArray = null;
        File datafile = null;
        String singleLine = null;
        S3Object s3object = null;
        boolean s3object_exists = false;
        Date date = new Date();
        System.out.println(date.toString());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf1 = new SimpleDateFormat("HH");
        System.out.println(sdf.format(date));
        System.out.println(sdf1.format(date));
        Date currentDate = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
        System.out.println(sdf1.format(currentDate));
        //if (shakehands_file.exists()) {
        try {
            s3object_exists = s3Client.doesObjectExist(aws_s3_bucket,aws_s3_data_file_key);
            if(s3object_exists) {
                s3object = s3Client.getObject(new GetObjectRequest(aws_s3_bucket, aws_s3_data_file_key));
                reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent(), "UTF8"));
                LOG.info("Sending record to Amazon Kinesis:");
                int count=0;
                //PutRecordRequest putRecord = new PutRecordRequest();
                PutRecordsRequest putRecords = new PutRecordsRequest();
                putRecords.setStreamName(streamName);
                List<PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>();
                String[] result=null;
                while ((singleLine = reader.readLine()) != null) {
                    long createTime = System.currentTimeMillis();
                    singleLine = singleLine + "\n";
                    result = singleLine.split("\001");

                   /* for (int x=0; x<result.length; x++) {
                        //out.print(result[x]);
                        //System.out.print(",");
                    }*/
                    //System.out.println();
                    byteArray = singleLine.getBytes("UTF-8");

                    if (byteArray == null) {
                        LOG.warn("Could not get bytes for Google Analytics data");
                        return;
                    }
                    PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
                    putRecordsRequestEntry.setData(ByteBuffer.wrap(byteArray));
                    putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", count));
                    putRecordsRequestEntryList.add(putRecordsRequestEntry);
                    if(putRecordsRequestEntryList.size()==500){
                        putRecords.setRecords(putRecordsRequestEntryList);
                        PutRecordsResult putRecordsResult  = kinesisClient.putRecords(putRecords);
                        Thread.sleep(500);

                        while (putRecordsResult.getFailedRecordCount() > 0) {
                            LOG.info("Start resending "+putRecordsResult.getFailedRecordCount()+" failed records");
                            final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
                            final List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();
                            for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
                                final PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList.get(i);
                                final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
                                if (putRecordsResultEntry.getErrorCode() != null) {
                                    failedRecordsList.add(putRecordRequestEntry);
                                }
                            }
                            putRecordsRequestEntryList = failedRecordsList;
                            putRecords.setRecords(putRecordsRequestEntryList);
                            Thread.sleep(500);
                            putRecordsResult = kinesisClient.putRecords(putRecords);

                        }
                        putRecordsRequestEntryList.clear();
                    }

                    //putRecords.setRecords(putRecordsRequestEntryList);
                    //
                    //System.out.println("Put Result" + putRecordsResult);


                    //putRecord.setPartitionKey(String.format("partitionKey-%d", createTime));
                    //putRecord.setData(ByteBuffer.wrap(byteArray));
                    //kinesisClient.putRecord(putRecord);
                    //LOG.info(singleLine);
                    count++;
                    if(count%10000==0 ){
                        LOG.info(count);
                    }
                }
                LOG.info("Sent data file completed, total "+count+" rows");
                reader.close();
            }else{
                LOG.info("Data file does not exist, wait for next check");
            }

            if(shakehands_file.exists()) {
                shakehands_file.delete();
                LOG.info("Shake_hands file deleted successfully");
            }
            else {
                LOG.info("Shake_hands file does not exist");
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw ioe;
        } catch (AmazonClientException ace) {
            LOG.warn("Error sending record to Amazon Kinesis.", ace);
            ace.printStackTrace();
            throw ace;
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        //}
        // else{
        //    LOG.warn("Shake hands file is not ready");
        //}
    }

    private static void sendGoogleAnalyticsS3(AmazonS3 s3Client, AmazonKinesis kinesisClient,
                                            String streamName) {


        LoadProp loadprop = null;

        String line;
        BufferedReader reader = null;
        byte[] byteArray;
        try {
            loadprop = new LoadProp();
            String aws_s3_bucket = loadprop.getProperty("aws_s3_bucket");
            String aws_s3_data_file_key = loadprop.getProperty("aws_s3_data_file_key");
            String aws_s3_shake_hands_file_key = loadprop.getProperty("aws_s3_shake_hands_file_key");
            String shake_hands_flag = loadprop.getProperty("shake_hands_flag");
            if(shake_hands_flag.equals("true")){

            }else{
                boolean s3object_exists = s3Client.doesObjectExist(aws_s3_bucket,aws_s3_data_file_key);
                if(s3object_exists) {
                    S3Object s3object = s3Client.getObject(new GetObjectRequest(aws_s3_bucket, aws_s3_data_file_key));
                    reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
                    while ((line = reader.readLine()) != null) {
                        long createTime = System.currentTimeMillis();
                        line = line + "\n";
                        byteArray = line.getBytes("UTF-8");
                        if (byteArray.length == 0) {
                            LOG.warn("Could not get bytes for Google Analytics data");
                            return;
                        }
                        PutRecordRequest putRecord = new PutRecordRequest();
                        putRecord.setStreamName(streamName);
                        putRecord.setPartitionKey(String.format("partitionKey-%d", createTime));
                        putRecord.setData(ByteBuffer.wrap(byteArray));
                        kinesisClient.putRecord(putRecord);
                        Thread.sleep(100);
                    }
                    LOG.info("Sent data file completed");
                }else {
                    LOG.info("Data file does not exist, wait for next check");
                }
            }
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }  catch (InterruptedException e) {
            e.printStackTrace();
        } catch (AmazonClientException ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args)  {
        checkUsage(args);
        String streamName = args[0];
        String regionName = args[1];
        // Reporting interval
        final long REPORTING_INTERVAL_MILLIS = 60000L; // 1 minute
        LoadProp loadprop = null;
        AmazonKinesis kinesisClient = null;
        AmazonS3 s3Client = null;
        long nextReportingTimeInMillis;
        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        try{
            Region region = RegionUtils.getRegion(regionName);
            if (region == null) {
                System.err.println(regionName + " is not a valid AWS region.");
                System.exit(1);
            }
            setLogLevels();

            AWSCredentials credentials = CredentialUtils.getCredentialsProvider().getCredentials();
            kinesisClient = new AmazonKinesisClient(credentials,
                    ConfigurationUtils.getClientConfigWithUserAgent());
            s3Client = new AmazonS3Client(credentials,
                    ConfigurationUtils.getClientConfigWithUserAgent());
            kinesisClient.setRegion(region);
            s3Client.setRegion(region);
            validateStream(kinesisClient, streamName);
            loadprop = new LoadProp();
        } catch(Exception e){
            e.printStackTrace();
            System.exit(1);
        }
        while(true) {
            try {
                if (loadprop.getProperty("send_from_remote_dir").equals("false")) {
                    sendGoogleAnalytics(kinesisClient, streamName);
                }
                else if(loadprop.getProperty("send_from_remote_dir").equals("true"))
                    sendGoogleAnalyticsS3data(s3Client,kinesisClient, streamName);

                Thread.sleep(60000);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

}
