package com.mx.core.mysql.connection;

import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public class PoolMysql {
    private static Connection connection;

    /**
     * Create a datasource for connection to db
     * @param mapCredentials
     * @return DataSource
     * @throws Exception
     */
    public static DataSource createDataSource(Map<String, String> mapCredentials) throws Exception {
        return DataSourceBuilder
                .create()
                .driverClassName("com.mysql.cj.jdbc.Driver")
                .username(mapCredentials.get("dbUser"))
                .password(mapCredentials.get("dbPass"))
                .url(mapCredentials.get("dbUrl"))
                .build();
    }

    public static JdbcTemplate createTemplate(DataSource ds) throws Exception {
        return new JdbcTemplate(ds);
    }

    private static Connection startConnection(Map<String, String> mapCredentials) {

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(mapCredentials.get("dbUrl"), mapCredentials.get("dbUser"), mapCredentials.get("dbPass"));
            connection.setAutoCommit(false);
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("Exception at getConnection: ".concat(e.getMessage()));
        }
        return connection;
    }

    /**
     * Create a Connection to db
     * @param mapCredentials
     * @return Connection
     */
    public static Connection getConnection(Map<String, String> mapCredentials, int dbTimeOut) throws Exception {

        if ( mapCredentials.get("dbUser") == null || mapCredentials.get("dbUser").isEmpty()
                || mapCredentials.get("dbPass") == null || mapCredentials.get("dbPass").isEmpty()
                || mapCredentials.get("dbUrl") == null || mapCredentials.get("dbUrl").isEmpty()) {
            throw new Exception("Uno o más parámetros no tienen valor, verifica tus datos");
        }

        if (connection == null || connection.isClosed() || !connection.isValid(dbTimeOut) ) {
            return connection = startConnection(mapCredentials);
        } else {
            return connection;
        }
    }

    public static void closeConnection() {
        try {
            if (connection != null ) {
                connection.close();
            }
        } catch ( Exception ex) {
            System.err.println("Exception at closeConnection ".concat(ex.getMessage()));
        }
    }

}
