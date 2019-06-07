package com.redislabs.riot.cli;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

import com.zaxxer.hikari.HikariDataSource;

import lombok.Data;
import picocli.CommandLine.Option;

@Data
public class DatabaseConnectionOptions {

	@Option(names = "--driver", description = "Fully qualified name of the JDBC driver. Auto-detected based on the URL by default.")
	private String driverClassName;
	@Option(names = "--url", description = "JDBC URL of the database.", required = true)
	private String url;
	@Option(names = "--username", description = "Login username of the database.")
	private String username;
	@Option(names = "--password", description = "Login password of the database.", interactive = true)
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
