package com.redislabs.riot.cli.db;

import java.util.Map;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import picocli.CommandLine.Option;

public class DatabaseExportOptions extends DatabaseOptions {

	@Option(names = "--sql", description = "Insert SQL statement", paramLabel = "<sql>")
	private String sql;
	@Option(names = "--no-assert-updates", description = "Disable insert verification")
	private boolean noAssertUpdates;

	public String getSql() {
		return sql;
	}

	public JdbcBatchItemWriter<Map<String, Object>> writer() {
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<Map<String, Object>>();
		builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
		builder.dataSource(dataSource());
		builder.sql(sql);
		builder.assertUpdates(!noAssertUpdates);
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		return writer;
	}

}
