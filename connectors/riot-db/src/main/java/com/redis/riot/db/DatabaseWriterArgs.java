package com.redis.riot.db;

import java.util.Map;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class DatabaseWriterArgs extends DatabaseArgs {

	public static final boolean DEFAULT_ASSERT_UPDATES = true;

	@Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
	private String sql;

	@Option(names = "--assert-updates", description = "Confirm every insert results in update of at least one row. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean assertUpdates = DEFAULT_ASSERT_UPDATES;

	public JdbcBatchItemWriter<Map<String, Object>> writer() {
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
		builder.itemSqlParameterSourceProvider(NullableSqlParameterSource::new);
		builder.dataSource(dataSource());
		builder.sql(sql);
		builder.assertUpdates(assertUpdates);
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		return writer;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public boolean isAssertUpdates() {
		return assertUpdates;
	}

	public void setAssertUpdates(boolean assertUpdates) {
		this.assertUpdates = assertUpdates;
	}

}
