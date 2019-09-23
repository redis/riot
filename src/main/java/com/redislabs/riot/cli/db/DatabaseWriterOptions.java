package com.redislabs.riot.cli.db;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import picocli.CommandLine.Option;

public class DatabaseWriterOptions {

	@Option(required = true, names = "--sql", description = "Insert SQL statement", paramLabel = "<sql>")
	private String sql;
	@Option(names = "--assert-updates", description = "Every insert updates at least one row (default: ${DEFAULT-VALUE})", negatable = true)
	private boolean assertUpdates = true;

	public JdbcBatchItemWriter<Map<String, Object>> writer(DataSource dataSource) {
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<Map<String, Object>>();
		builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
		builder.dataSource(dataSource);
		builder.sql(sql);
		builder.assertUpdates(assertUpdates);
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		return writer;
	}

}
