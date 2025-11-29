// src/main/java/com/example/config/DatabaseConfig.java
package com.example.config;

import com.example.qualifier.SinkDatabase;
import com.example.qualifier.SourceDatabase;
import io.quarkus.agroal.DataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import java.sql.Connection;
import java.sql.SQLException;

@ApplicationScoped
public class DatabaseConfig {

    @DataSource("source")
    javax.sql.DataSource sourceDataSource;

    @DataSource("sink") 
    javax.sql.DataSource sinkDataSource;

    @Produces
    @Singleton
    @SourceDatabase
    public Connection produceSourceConnection() throws SQLException {
        return sourceDataSource.getConnection();
    }

    @Produces
    @Singleton
    @SinkDatabase
    public Connection produceSinkConnection() throws SQLException {
        return sinkDataSource.getConnection();
    }
}