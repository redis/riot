package com.redislabs.riot.cli.out;

import java.util.Map;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import com.redislabs.riot.cli.DatabaseConnectionOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "db", description = "Export to a database")
public class DatabaseExport extends AbstractExportWriterCommand {

	@Mixin
	private DatabaseConnectionOptions connection = new DatabaseConnectionOptions();

	@Option(names = "--sql", description = "Insert query, e.g. \"INSERT INTO people (id, name) VALUES (:id, :name)\"", required = true)
	private String sql;

	@Override
	protected JdbcBatchItemWriter<Map<String, Object>> writer() {
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<Map<String, Object>>();
		builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
		builder.dataSource(connection.dataSource());
		builder.sql(sql);
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		return writer;
	}

}
