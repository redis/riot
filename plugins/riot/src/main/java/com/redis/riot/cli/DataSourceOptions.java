package com.redis.riot.cli;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import picocli.CommandLine.Option;

import javax.sql.DataSource;

public class DataSourceOptions {

	@Option(names = "--driver", description = "Fully qualified name of the JDBC driver.", paramLabel = "<class>")
	private String driver;
	@Option(names = "--url", required = true, description = "JDBC URL to connect to the database.", paramLabel = "<string>")
	private String url;
	@Option(names = "--username", description = "Login username of the database.", paramLabel = "<string>")
	private String username;
	@Option(names = "--password", arity = "0..1", interactive = true, description = "Login password of the database.", paramLabel = "<pwd>")
	private String password;

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

	public DataSource dataSource() {
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl(url);
		properties.setDriverClassName(driver);
		properties.setUsername(username);
		properties.setPassword(password);
		return properties.initializeDataSourceBuilder().build();
	}

	@Override
	public String toString() {
		return "DataSourceOptions [driver=" + driver + ", url=" + url + ", username=" + username + ", password="
				+ password + "]";
	}

}
