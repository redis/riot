package com.redis.riot.db;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

public interface DatabaseUtils {

    static DataSource dataSource(DataSourceOptions options) {
        DataSourceProperties properties = new DataSourceProperties();
        properties.setUrl(options.getUrl());
        properties.setDriverClassName(options.getDriver());
        properties.setUsername(options.getUsername());
        properties.setPassword(options.getPassword());
        return properties.initializeDataSourceBuilder().build();
    }

}
