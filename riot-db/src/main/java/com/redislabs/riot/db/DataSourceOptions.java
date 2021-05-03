package com.redislabs.riot.db;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import picocli.CommandLine.Option;

import javax.sql.DataSource;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataSourceOptions {

	@Option(names = "--driver", description = "Fully qualified name of the JDBC driver", paramLabel = "<class>")
	private String driver;
	@Option(names = "--url", required = true, description = "JDBC URL to connect to the database", paramLabel = "<string>")
	private String url;
	@Option(names = "--username", description = "Login username of the database", paramLabel = "<string>")
	private String username;
	@Option(names = "--password", arity = "0..1", interactive = true, description = "Login password of the database", paramLabel = "<pwd>")
	private String password;

	public DataSource dataSource() {
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl(url);
		properties.setDriverClassName(driver);
		properties.setUsername(username);
		properties.setPassword(password);
		return properties.initializeDataSourceBuilder().build();
	}

}
