package com.redislabs.riot.cli.db;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

import com.redislabs.riot.Riot;
import com.redislabs.riot.cli.HelpAwareCommand;
import com.zaxxer.hikari.HikariDataSource;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(name = "db", description = "Database import/export", subcommands = { DatabaseImportCommand.class,
		DatabaseExportCommand.class })
public class DatabaseConnector extends HelpAwareCommand {

	private final Logger log = LoggerFactory.getLogger(DatabaseConnector.class);

	@ParentCommand
	private Riot riot;
	@Option(names = { "-d",
			"--driver" }, description = "Fully qualified name of the JDBC driver", paramLabel = "<class>")
	private String driver;
	@Option(names = { "-u",
			"--url" }, required = true, description = "URL to connect to the database", paramLabel = "<string>")
	private String url;
	@Option(names = { "-n", "--username" }, description = "Login username of the database", paramLabel = "<string>")
	private String username;
	@Option(names = { "-p",
			"--password" }, arity = "0..1", interactive = true, description = "Login password of the database", paramLabel = "<pwd>")
	private String password;

	protected DataSource dataSource() {
		DataSourceProperties properties = new DataSourceProperties();
		properties.setUrl(url);
		properties.setDriverClassName(driver);
		properties.setUsername(username);
		properties.setPassword(password);
		log.debug("Initializing datasource: driver={} url={}", driver, url);
		return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
	}

	public Riot riot() {
		return riot;
	}

}
