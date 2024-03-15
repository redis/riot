package com.redis.riot.db;

import java.util.Map;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;

import com.redis.riot.core.AbstractMapExport;

public class DatabaseExport extends AbstractMapExport {

	public static final boolean DEFAULT_ASSERT_UPDATES = true;

	private String sql;
	private DataSourceOptions dataSourceOptions = new DataSourceOptions();
	private boolean assertUpdates = DEFAULT_ASSERT_UPDATES;

	public void setSql(String sql) {
		this.sql = sql;
	}

	public void setDataSourceOptions(DataSourceOptions dataSourceOptions) {
		this.dataSourceOptions = dataSourceOptions;
	}

	public void setAssertUpdates(boolean assertUpdates) {
		this.assertUpdates = assertUpdates;
	}

	@Override
	protected JdbcBatchItemWriter<Map<String, Object>> writer() {
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
		builder.itemSqlParameterSourceProvider(NullableSqlParameterSource::new);
		builder.dataSource(dataSourceOptions.dataSource());
		builder.sql(sql);
		builder.assertUpdates(assertUpdates);
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		return writer;
	}

	private static class NullableSqlParameterSource extends MapSqlParameterSource {

		public NullableSqlParameterSource(@Nullable Map<String, ?> values) {
			super(values);
		}

		@Override
		@Nullable
		public Object getValue(String paramName) {
			if (!hasValue(paramName)) {
				return null;
			}
			return super.getValue(paramName);
		}

	}

}
