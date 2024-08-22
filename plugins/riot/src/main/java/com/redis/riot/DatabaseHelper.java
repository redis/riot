package com.redis.riot;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

public abstract class DatabaseHelper {

	private DatabaseHelper() {
	}

	public static DataSource dataSource(DataSourceArgs args) {
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl(args.getUrl());
		properties.setDriverClassName(args.getDriver());
		properties.setUsername(args.getUsername());
		properties.setPassword(args.getPassword());
		return properties.initializeDataSourceBuilder().build();
	}

}
