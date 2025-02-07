package com.redis.riot;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.redis.riot.core.RiotException;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "db-export", description = "Export Redis data to a relational database.")
public class DatabaseExport extends AbstractRedisExportCommand {

	public static final boolean DEFAULT_ASSERT_UPDATES = true;

	@ArgGroup(exclusive = false)
	private DataSourceArgs dataSourceArgs = new DataSourceArgs();

	@Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
	private String sql;

	@Option(names = "--assert-updates", description = "Confirm every insert results in update of at least one row. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean assertUpdates = DEFAULT_ASSERT_UPDATES;

	@Override
	protected Job job() {
		return job(step(writer()).processor(mapProcessor()));
	}

	private JdbcBatchItemWriter<Map<String, Object>> writer() {
		Assert.hasLength(sql, "No SQL statement specified");
		log.info("Creating data source with {}", dataSourceArgs);
		DataSource dataSource;
		try {
			dataSource = dataSourceArgs.dataSource();
		} catch (Exception e) {
			throw new RiotException(e);
		}
		log.info("Creating JDBC writer with sql=\"{}\" assertUpdates={}", sql, assertUpdates);
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
		builder.itemSqlParameterSourceProvider(NullableSqlParameterSource::new);
		builder.dataSource(dataSource);
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

	public DataSourceArgs getDataSourceArgs() {
		return dataSourceArgs;
	}

	public void setDataSourceArgs(DataSourceArgs dataSourceArgs) {
		this.dataSourceArgs = dataSourceArgs;
	}

}
