package io.prestosql.gateway.persistence;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.annotation.Nullable;

/**
 * Config for the database which we want to connect to store
 * metadata and stats.
 */
public class DataStoreConfig
{
    private String jdbcUrl;
    private String databaseUser;
    private String password;
    private String driver;
    private String hikariCPPropsFile;
    private int maximumPoolSize;
    private long poolConnectionTimeout;
    private long poolIdleTimeout;

    // This property is used if the HikariCP properties file is not specified
    @Nullable
    @Config("jdbcUrl")
    @ConfigDescription("jdbcUrl of the database")
    public DataStoreConfig setJdbcUrl(String url)
    {
        this.jdbcUrl = url;
        return this;
    }

    // This property is used if the HikariCP properties file is not specified
    @Nullable
    @Config("user")
    @ConfigDescription("username of the database")
    public DataStoreConfig setDatabaseUser(String databaseUser)
    {
        this.databaseUser = databaseUser;
        return this;
    }

    // This property is used if the HikariCP properties file is not specified
    @Nullable
    @Config("password")
    @ConfigDescription("password of the database")
    public DataStoreConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    // This property is used if the HikariCP properties file is not specified
    @Nullable
    @Config("driver")
    @ConfigDescription("Driver of the database")
    public DataStoreConfig setDriver(String driver)
    {
        this.driver = driver;
        return this;
    }

    // This property is used if the HikariCP properties file is not specified
    @Nullable
    @Config("maximumPoolSize")
    @ConfigDescription("Max size of the pool. This property is used if the HikariCP properties file is not specified")
    public DataStoreConfig setMaximumPoolSize(int maximumPoolSize)
    {
        this.maximumPoolSize = maximumPoolSize;
        return this;
    }

    @Nullable
    @Config("hikariCPPropsFile")
    @ConfigDescription("Property file for HikariCP properties")
    public DataStoreConfig setHikariCPPropsFile(String hikariCPPropsFile)
    {
        this.hikariCPPropsFile = hikariCPPropsFile;
        return this;
    }

    @Nullable
    @Config("poolConnectionTimeout")
    @ConfigDescription("Connection timeout of the pool. This property is used if the HikariCP properties file is not specified")
    public DataStoreConfig setPoolConnectionTimeout(long poolConnectionTimeout)
    {
        this.poolConnectionTimeout = poolConnectionTimeout;
        return this;
    }

    @Nullable
    @Config("poolIdleTimeout")
    @ConfigDescription("Idle timeout of the pool. This property is used if the HikariCP properties file is not specified")
    public DataStoreConfig setPoolIdleTimeout(long poolIdleTimeout)
    {
        this.poolIdleTimeout = poolIdleTimeout;
        return this;
    }

    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    public String getDatabaseUser()
    {
        return databaseUser;
    }

    public String getPassword()
    {
        return password;
    }

    public String getDriver()
    {
        return driver;
    }

    public String getHikariCPPropsFile()
    {
        return hikariCPPropsFile;
    }

    public int getMaximumPoolSize()
    {
        return maximumPoolSize;
    }

    public long getPoolConnectionTimeout()
    {
        return poolConnectionTimeout;
    }

    public long getPoolIdleTimeout()
    {
        return poolIdleTimeout;
    }
}
