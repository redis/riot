package com.redislabs.riot.cli.db;

import java.util.Map;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import lombok.Data;
import lombok.EqualsAndHashCode;
import picocli.CommandLine.Option;

@EqualsAndHashCode(callSuper = true)
public @Data class DatabaseWriterOptions extends DatabaseOptions {

	@Option(required = true, names = "--sql", description = "Insert SQL statement", paramLabel = "<sql>")
	private String sql;
	@Option(names = "--assert-updates", description = "Every insert updates at least one row (default: ${DEFAULT-VALUE})", negatable = true)
	private boolean assertUpdates = true;

	public JdbcBatchItemWriter<Map<String, Object>> writer() {
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<Map<String, Object>>();
		builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
		builder.dataSource(dataSource());
		builder.sql(sql);
		builder.assertUpdates(assertUpdates);
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		return writer;
	}

}
