package com.redis.riot.cli;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;

import com.redis.riot.cli.common.AbstractExportCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.DatabaseHelper;
import com.redis.riot.cli.common.DbExportOptions;
import com.redis.riot.core.processor.DataStructureToMapProcessor;
import com.redis.spring.batch.RedisItemReader;

import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "db-export", description = "Export Redis data to a relational database.")
public class DbExport extends AbstractExportCommand {

	private static final String TASK_NAME = "Exporting to database";

	@Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
	private String sql;
	@Mixin
	private DbExportOptions dbExportOptions = new DbExportOptions();

	public DbExportOptions getDbExportOptions() {
		return dbExportOptions;
	}

	public void setDbExportOptions(DbExportOptions exportOptions) {
		this.dbExportOptions = exportOptions;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	@Override
	protected Job job(CommandContext context) {
		DataSource dataSource = DatabaseHelper.dataSource(dbExportOptions.getDataSourceOptions());
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
		builder.itemSqlParameterSourceProvider(NullableSqlParameterSource::new);
		builder.dataSource(dataSource);
		builder.sql(sql);
		builder.assertUpdates(dbExportOptions.isAssertUpdates());
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		RedisItemReader<String, String> reader = reader(context, StringCodec.UTF8).struct();
		DataStructureToMapProcessor processor = DataStructureToMapProcessor.of(dbExportOptions.getKeyRegex());
		return job(step(reader, writer).task(TASK_NAME).processor(processor));
	}

	@Override
	public String toString() {
		return "DbExport [sql=" + sql + ", dbExportOptions=" + dbExportOptions + ", readerOptions=" + readerOptions
				+ ", jobOptions=" + jobOptions + "]";
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
