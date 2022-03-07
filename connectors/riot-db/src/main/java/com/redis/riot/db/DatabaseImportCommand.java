package com.redis.riot.db;

import java.sql.Connection;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import com.redis.riot.AbstractImportCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "import", description = "Import from a database")
public class DatabaseImportCommand extends AbstractImportCommand {

	private static final Logger log = LoggerFactory.getLogger(DatabaseImportCommand.class);

	private static final String NAME = "db-import";

	@CommandLine.Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
	private String sql;
	@Mixin
	private DataSourceOptions dataSourceOptions = new DataSourceOptions();
	@Mixin
	private DatabaseImportOptions importOptions = new DatabaseImportOptions();

	public DataSourceOptions getDataSourceOptions() {
		return dataSourceOptions;
	}

	@Override
	protected Job job(JobBuilder jobBuilder) throws Exception {
		log.debug("Creating data source: {}", dataSourceOptions);
		DataSource dataSource = dataSourceOptions.dataSource();
		try (Connection connection = dataSource.getConnection()) {
			String name = connection.getMetaData().getDatabaseProductName();
			log.debug("Creating {} database reader: {}", name, importOptions);
			JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<>();
			builder.saveState(false);
			builder.dataSource(dataSource);
			builder.name(name + "-database-reader");
			builder.rowMapper(new ColumnMapRowMapper());
			builder.sql(sql);
			importOptions.configure(builder);
			JdbcCursorItemReader<Map<String, Object>> reader = builder.build();
			reader.afterPropertiesSet();
			return jobBuilder.start(step(NAME, "Importing from " + name, reader).build()).build();
		}
	}

}
