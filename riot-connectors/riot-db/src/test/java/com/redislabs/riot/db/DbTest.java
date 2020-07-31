package com.redislabs.riot.db;

import com.redislabs.riot.test.BaseTest;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.sql.DataSource;

public class DbTest extends BaseTest {

    @Override
    protected int execute(String[] args) {
        return new RiotDb().execute(args);
    }

    @Override
    protected String applicationName() {
        return "riot-db";
    }

    protected DataSource dataSource(JdbcDatabaseContainer container) {
        DataSourceProperties properties = new DataSourceProperties();
        properties.setUrl(container.getJdbcUrl());
        properties.setUsername(container.getUsername());
        properties.setPassword(container.getPassword());
        return properties.initializeDataSourceBuilder().build();
    }
}
