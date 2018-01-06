/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.e1ef.a2r.service.kinesis.processor;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.*;

/**
 * Processes records retrieved from stock trades stream.
 *
 */
public class GoogleAnalyticsRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(GoogleAnalyticsRecordProcessor.class);
    private String kinesisShardId;
    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    private int count = 0;
    // Reporting interval
    private static final long REPORTING_INTERVAL_MILLIS = 60000L; // 1 minute
    //private long nextReportingTimeInMillis;

    // Checkpointing interval
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L; // 1 minute
    private long nextCheckpointTimeInMillis;

    // Aggregates stats for stock trades
    //private StockStats stockStats = new StockStats();
    private static String driverName = "com.facebook.presto.jdbc.PrestoDriver";
    private Connection con = null;
    private Statement stmt = null;
    PreparedStatement prep = null;
    private String insertSQL = "";

    public void initialize(InitializationInput initializationInput) {

        try {
            LOG.info("Initializing record processor for shard: " + initializationInput.getShardId());

            this.kinesisShardId = initializationInput.getShardId();
            //nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
            Class.forName(driverName);
            con = DriverManager.getConnection("jdbc:presto://10.163.25.209:8889/hive/traffic","leo.wang",null);
            stmt = con.createStatement();
        }catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void processRecords(ProcessRecordsInput processRecordsInput) {
        try {
            String data = null;
            String[] result=null;
            String full_SQL = "";
            String prefix_insertSQL = "insert into traffic.test(column1,column2) values";
            for (Record record : processRecordsInput.getRecords()) {
                data = decoder.decode(record.getData()).toString();
                result=data.split("\001");
                count++;
                if((count%5000!=0) && (result.length>=3)) {
                    insertSQL = insertSQL + "('"+result[0]+"','"+result[1]+"'), \n" ;
                }else if((count%5000==0) && (result.length>=3)) {
                    insertSQL = insertSQL + "('"+result[0]+"','"+result[1]+"') \n" ;
                    full_SQL = prefix_insertSQL + insertSQL;
                    stmt.executeUpdate(full_SQL);
                    insertSQL = "";
                    LOG.info(count);
                }
                //processRecord(record);

            }



        }catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }



        // If it is time to report stats as per the reporting interval, report stats
       /* if (System.currentTimeMillis() > nextReportingTimeInMillis) {
            reportStats();
            //resetStats();
            nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        }*/

        // Checkpoint once every checkpoint interval
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(processRecordsInput.getCheckpointer());
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    private void processRecord(Record record) {
        String data = null;
        String[] result=null;
            try {
                data = decoder.decode(record.getData()).toString();
                result=data.split("\001");
                if(result.length>=3) {
                    //System.out.println(result[0]);
                    //System.out.println(result[1]);
                    //System.out.println(result[2]);
                    insertSQL = "insert into traffic.test(column1,column2) values('"+result[0]+"','"+result[1]+"')";
                    stmt.executeUpdate(insertSQL);
                }
                if (data == null) {
                    LOG.warn("Skipping record. Unable to parse GA record. Partition Key: " + record.getPartitionKey());
                    return;
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
    }


    public void shutdown(ShutdownInput shutdownInput) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.getCheckpointer());
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            LOG.error("Caught throttling exception, skipping checkpoint.", e);
        } catch (InvalidStateException e) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
    }

}
