package io.prestosql.gateway.persistence;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;
import org.javalite.activejdbc.Base;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

@Singleton
public class JDBCConnectionManager
{
    private static HikariDataSource hikariDataSource;
    Logger log = Logger.get(JDBCConnectionManager.class);

    @Inject
    public JDBCConnectionManager(DataStoreConfig configuration)
            throws ClassNotFoundException
    {
        DataStoreConfig conf = requireNonNull(configuration, "DataStoreConfig is null");
        HikariConfig hc;
        if (isNullOrEmpty(conf.getHikariCPPropsFile())) {
            hc = new HikariConfig();
            Class.forName(conf.getDriver());
            hc.setDriverClassName(conf.getDriver());
            hc.setJdbcUrl(conf.getJdbcUrl());
            hc.setUsername(conf.getDatabaseUser());
            hc.setPassword(conf.getPassword());
            if (conf.getMaximumPoolSize() > 0) {
                hc.setMaximumPoolSize(conf.getMaximumPoolSize());
            }
            if (conf.getPoolConnectionTimeout() > 0) {
                hc.setConnectionTimeout(conf.getPoolConnectionTimeout());
            }
            if (conf.getPoolIdleTimeout() > 0) {
                hc.setIdleTimeout(conf.getPoolIdleTimeout());
            }
        }
        else {
            hc = new HikariConfig(conf.getHikariCPPropsFile());
        }
        hc.addDataSourceProperty("stringtype", "unspecified");
        this.hikariDataSource = new HikariDataSource(hc);
    }

    public void open()
    {
        Base.open(this.hikariDataSource);
    }

    public void openTransaction()
    {
        Base.open(this.hikariDataSource);
        Connection con = Base.connection();
        try {
            Base.connection().setAutoCommit(false);
        }
        catch (SQLException se) {
            log.warn(se, "Caught exception while starting transaction.");
        }
    }

    public void commitTransaction()
    {
        try {
            Base.connection().commit();
        }
        catch (SQLException se) {
            log.warn(se, "Caught exception while commiting transaction.");
        }
    }

    public void rollbackTransaction()
    {
        try {
            Base.connection().rollback();
        }
        catch (SQLException se) {
            log.warn(se, "Caught exception while rolling back transaction.");
        }
    }

    public void close()
    {
        if (Base.hasConnection()) {
            Base.close();
        }
    }

    private boolean isNullOrEmpty(String s)
    {
        return s == null || s.isEmpty();
    }
}
