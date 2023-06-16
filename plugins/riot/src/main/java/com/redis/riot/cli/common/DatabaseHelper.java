package com.redis.riot.cli.common;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

import com.redis.riot.cli.db.DataSourceOptions;

public interface DatabaseHelper {

	static DataSource dataSource(DataSourceOptions options) {
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl(options.getUrl());
		properties.setDriverClassName(options.getDriver());
		properties.setUsername(options.getUsername());
		properties.setPassword(options.getPassword());
		return properties.initializeDataSourceBuilder().build();
	}

}
