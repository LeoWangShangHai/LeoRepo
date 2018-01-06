package com.e1ef.a2r.service.kinesis.presto;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PrestoJdbcClient {
    private static String driverName = "com.facebook.presto.jdbc.PrestoDriver";
    private static final Log LOG = LogFactory.getLog(PrestoJdbcClient.class);
    private static final Logger ROOT_LOGGER = Logger.getLogger("");
    private static final Logger PROCESSOR_LOGGER = Logger.getLogger("com.e1ef.a2r.service.kinesis.presto");
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
        LOG.info("Main Started");
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        //replace "hive" here with the name of the user the queries should run as
        Connection con = DriverManager.getConnection("jdbc:presto://10.163.25.209:8889/hive/default","leo.wang",null);
        Statement stmt = con.createStatement();
        String sql = ("select * from testHiveDriverTable");
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

}
