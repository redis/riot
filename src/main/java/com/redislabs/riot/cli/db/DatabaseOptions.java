package com.redislabs.riot.cli.db;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Option;

@Slf4j
public class DatabaseOptions {

	@Option(names = "--db-driver", description = "Fully qualified name of the JDBC driver", paramLabel = "<class>")
	private String driver;
	@Option(names = "--db-url", required = true, description = "JDBC URL to connect to the database", paramLabel = "<string>")
	private String url;
	@Option(names = "--db-username", description = "Login username of the database", paramLabel = "<string>")
	private String username;
	@Option(names = "--db-password", arity = "0..1", interactive = true, description = "Login password of the database", paramLabel = "<pwd>")
	private String password;

	public String getUrl() {
		return url;
	}

	protected DataSource dataSource() {
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl(url);
		properties.setDriverClassName(driver);
		properties.setUsername(username);
		properties.setPassword(password);
		log.debug("Initializing datasource: driver={} url={}", driver, url);
		return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
	}

}
