package com.redislabs.riot.db;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.redislabs.riot.RiotApp;
import com.redislabs.riot.test.AbstractStandaloneRedisTest;

public abstract class AbstractDatabaseTest extends AbstractStandaloneRedisTest {

	@Override
	protected RiotApp app() {
		return new RiotDb();
	}

	@Override
	protected String appName() {
		return "riot-db";
	}

	@SuppressWarnings("rawtypes")
	protected DataSource dataSource(JdbcDatabaseContainer container) {
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl(container.getJdbcUrl());
		properties.setUsername(container.getUsername());
		properties.setPassword(container.getPassword());
		return properties.initializeDataSourceBuilder().build();
	}
}
