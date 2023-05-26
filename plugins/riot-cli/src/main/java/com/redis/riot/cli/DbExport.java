package com.redis.riot.cli;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;

import com.redis.riot.cli.common.AbstractExportCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.db.DataSourceOptions;
import com.redis.riot.cli.db.DbExportOptions;
import com.redis.riot.cli.db.NullableSqlParameterSource;
import com.redis.riot.core.processor.DataStructureToMapProcessor;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.DataStructure;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "db-export", description = "Export Redis data to a relational database.")
public class DbExport extends AbstractExportCommand {

	private static final String TASK_NAME = "Exporting to database";
	private static Logger log = Logger.getLogger(DbExport.class.getName());

	@Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
	private String sql;
	@Mixin
	private DataSourceOptions dataSourceOptions = new DataSourceOptions();
	@Mixin
	private DbExportOptions options = new DbExportOptions();

	public DbExportOptions getOptions() {
		return options;
	}

	public void setOptions(DbExportOptions exportOptions) {
		this.options = exportOptions;
	}

	public DataSourceOptions getDataSourceOptions() {
		return dataSourceOptions;
	}

	public void setDataSourceOptions(DataSourceOptions dataSourceOptions) {
		this.dataSourceOptions = dataSourceOptions;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	@Override
	protected Job job(CommandContext context) {
		log.log(Level.FINE, "Creating data source with {0}", dataSourceOptions);
		DataSource dataSource = dataSourceOptions.dataSource();
		log.log(Level.FINE, "Creating writer for database with {0}", options);
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
		builder.itemSqlParameterSourceProvider(NullableSqlParameterSource::new);
		builder.dataSource(dataSource);
		builder.sql(sql);
		builder.assertUpdates(options.isAssertUpdates());
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		ItemProcessor<DataStructure<String>, Map<String, Object>> processor = DataStructureToMapProcessor
				.of(options.getKeyRegex());
		RedisItemReader<String, String, DataStructure<String>> reader = reader(context);
		return job(context, step(context, reader, processor, writer), TASK_NAME);
	}

}
