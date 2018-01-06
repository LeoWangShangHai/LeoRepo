package com.e1ef.a2r.service.kinesis.hive;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HiveJdbcClient {
    private static String driverName = "com.amazon.hive.jdbc41.HS2Driver";
    private static final Log LOG = LogFactory.getLog(HiveJdbcClient.class);
    private static final Logger ROOT_LOGGER = Logger.getLogger("");
    private static final Logger PROCESSOR_LOGGER = Logger.getLogger("com.e1ef.a2r.service.kinesis.hive");
    /**
     * @param args
     * @throws SQLException
     */
    /**
     * Sets the global log level to WARNING and the log level for this package to INFO,
     * so that we only see INFO messages for this processor. This is just for the purpose
     * of this tutorial, and should not be considered as best practice.
     */
    private static void setLogLevels() {
        ROOT_LOGGER.setLevel(Level.ALL);
        PROCESSOR_LOGGER.setLevel(Level.ALL);
    }
    public static void main(String[] args) throws SQLException {
        setLogLevels();
        LOG.info("Shake_hands file deleted successfully");
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        //replace "hive" here with the name of the user the queries should run as
        Connection con = DriverManager.getConnection("jdbc:hive2://10.163.25.209:10000;AuthMech=3;UID=leo.wang;PWD=leo.wang");
        Statement stmt = con.createStatement();
        LOG.info(con.getMetaData().supportsBatchUpdates());
        String sql = ("show tables");
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        String tableName = "testHiveDriverTable";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key int, value string)");
        // show tables
        // String sql = "show tables '" + tableName + "'";
        sql = ("show tables");
        rs = stmt.executeQuery(sql);
        while(rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

}
