package com.redislabs.riot.cli;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

@Command(name = "db", description = "Database")
public class DatabaseWriterCommand extends AbstractCommand {

	private final static Logger log = LoggerFactory.getLogger(DatabaseWriterCommand.class);

	@ParentCommand
	private AbstractReaderCommand parent;

	@ArgGroup(exclusive = false, heading = "Database connection%n")
	private DatabaseConnectionOptions connection = new DatabaseConnectionOptions();

	@Parameters(description = "Query to write entries e.g. \"INSERT INTO people (id, name) VALUES (:id, :name)\"", paramLabel = "<sql>")
	private String sql;

	@Override
	public void run() {
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<Map<String, Object>>();
		builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
		builder.dataSource(connection.dataSource());
		builder.sql(sql);
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		try {
			parent.execute(writer, "database");
		} catch (Exception e) {
			System.err.println(e.getMessage());
			log.debug("Could not execute transfer", e);
		}
	}
}
