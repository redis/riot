package com.redislabs.riot.cli.db;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import com.redislabs.riot.cli.ExportCommand;
import com.zaxxer.hikari.HikariDataSource;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "db", description = "Export to database")
public class DatabaseExportCommand extends ExportCommand {

	@Mixin
	private DatabaseConnectionOptions connection = new DatabaseConnectionOptions();
	@Option(names = { "-s",
			"--sql" }, required = true, description = "Insert/update statement e.g. \"INSERT INTO people (id, name) VALUES (:ssn, :name)\"")
	private String sql;

	@Override
	protected ItemWriter<Map<String, Object>> writer() {
		HikariDataSource dataSource = connection.dataSource();
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<Map<String, Object>>();
		builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
		builder.dataSource(dataSource);
		builder.sql(sql);
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		return writer;
	}

}
