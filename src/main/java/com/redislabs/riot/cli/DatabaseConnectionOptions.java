package com.redislabs.riot.cli;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

import com.zaxxer.hikari.HikariDataSource;

import picocli.CommandLine.Option;

public class DatabaseConnectionOptions {

	@Option(names = "--jdbc-driver", description = "Fully qualified name of the JDBC driver", paramLabel = "<class>")
	private String driverClassName;
	@Option(names = "--url", required = true, description = "JDBC URL of the database")
	private String url;
	@Option(names = "--username", description = "Login username of the database")
	private String username;
	@Option(names = "--password", arity = "0..1", interactive = true, description = "Login password of the database")
	private String password;

	public DataSource dataSource() {
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl(url);
		properties.setDriverClassName(driverClassName);
		properties.setUsername(username);
		properties.setPassword(password);
		return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
	}

}
