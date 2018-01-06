package com.e1ef.a2r.service.kinesis.hive;


import java.sql.*;
public class AmazonJDBCHive {
    // Define a string as the fully qualified class name
// (FQCN) of the desired JDBC driver
    static String JDBCDriver =
            "com.amazon.hive.jdbc41.HS2Driver";
    // Define a string as the connection URL
    private static final String CONNECTION_URL =
            "jdbc:hive2://10.163.25.209:10000;AuthMech=3;UID=leo.wang;PWD=leo.wang";
    public static void main(String[] args) {
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        PreparedStatement prep = null;
        String query = "SELECT first_name, last_name, emp_id FROM default.emp";

        String prepQuery = "SELECT first_name, last_name, emp_id FROM default.emp where store_id = ?";
        try {
// Register the driver using the class name
            Class.forName(JDBCDriver);
// Establish a connection using the connection
// URL
            con = DriverManager.getConnection(CONNECTION_URL);
// Create a Statement object for sending SQL
// statements to the database
            stmt = con.createStatement();
// Execute the SQL statement
            rs = stmt.executeQuery(query);
// Display a header line for output appearing in
// the Console View
            System.out.printf("%20s%20s%20s\r\n", "FIRST NAME", "LAST NAME" , "EMPLOYEE ID");
// Step through each row in the result set
// returned from the database
            while(rs.next()) {
// Retrieve values from the row where the
// cursor is currently positioned using
// column names
                String FirstName = rs.getString("first_name");
                        String LastName = rs.getString("last_name");
                String EmployeeID = rs.getString("emp_id");

                System.out.printf("%20s%20s%20s\r\n",FirstName, LastName, EmployeeID);
            }
            prep = con.prepareStatement
                    (prepQuery);

            prep.setInt(1, 204);

            prep.execute();

            while(rs.next()) {

                String FirstName = rs.getString("first_name");
                String LastName = rs.getString("last_name");
                String EmployeeID = rs.getString("emp_id");
                System.out.printf("%20s%20s%20s\r\n",
                        FirstName, LastName, EmployeeID);
            }
        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException se1) {
// Log this
            }
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException se2) {
// Log this
            }try {
                if (prep != null) {
                    prep.close();
                }
            } catch (SQLException se3) {
// Log this
            }
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException se4) {
// Log this
            } // End try
        } // End try
    } // End main
} // End AmazonJDBCHiveExample