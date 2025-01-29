package com.redis.riot;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.jdbc.EmbeddedDatabaseConnection;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class DataSourceArgs {

	@Option(names = "--jdbc-driver", description = "Fully qualified name of the JDBC driver (e.g. oracle.jdbc.OracleDriver).", paramLabel = "<class>")
	private String driver;

	@Option(names = "--jdbc-url", required = true, description = "JDBC URL to connect to the database.", paramLabel = "<string>")
	private String url;

	@Option(names = "--jdbc-user", description = "Login username of the database.", paramLabel = "<string>")
	private String username;

	@Option(names = "--jdbc-pass", arity = "0..1", interactive = true, description = "Login password of the database.", paramLabel = "<pwd>")
	private String password;

	public DataSource dataSource() throws Exception {
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl(url);
		properties.setDriverClassName(driver);
		properties.setUsername(username);
		properties.setPassword(password);
		properties.setEmbeddedDatabaseConnection(EmbeddedDatabaseConnection.NONE);
		properties.afterPropertiesSet();
		return properties.initializeDataSourceBuilder().build();
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

}
