package com.redis.riot.db;

import java.sql.Connection;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import com.redis.riot.AbstractImportCommand;
import com.redis.riot.MapProcessorOptions;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@Command(name = "import", description = "Import from a database")
public class DatabaseImportCommand extends AbstractImportCommand<Map<String, Object>, Map<String, Object>> {

	private static final String NAME = "db-import";

	@CommandLine.Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
	private String sql;
	@Mixin
	private DataSourceOptions dataSourceOptions = new DataSourceOptions();
	@Mixin
	private DatabaseImportOptions importOptions = new DatabaseImportOptions();
	@CommandLine.ArgGroup(exclusive = false, heading = "Processor options%n")
	private MapProcessorOptions processorOptions = new MapProcessorOptions();

	@Override
	protected Flow flow() throws Exception {
		log.debug("Creating data source: {}", dataSourceOptions);
		DataSource dataSource = dataSourceOptions.dataSource();
		try (Connection connection = dataSource.getConnection()) {
			String name = connection.getMetaData().getDatabaseProductName();
			log.debug("Creating {} database reader: {}", name, importOptions);
			JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<>();
			builder.saveState(false);
			builder.dataSource(dataSource);
			if (importOptions.getFetchSize() != null) {
				builder.fetchSize(importOptions.getFetchSize());
			}
			if (importOptions.getMaxRows() != null) {
				builder.maxRows(importOptions.getMaxRows());
			}
			builder.name(name + "-database-reader");
			if (importOptions.getQueryTimeout() != null) {
				builder.queryTimeout(importOptions.getQueryTimeout());
			}
			builder.rowMapper(new ColumnMapRowMapper());
			builder.sql(sql);
			builder.useSharedExtendedConnection(importOptions.isUseSharedExtendedConnection());
			builder.verifyCursorPosition(importOptions.isVerifyCursorPosition());
			JdbcCursorItemReader<Map<String, Object>> reader = builder.build();
			reader.afterPropertiesSet();
			return flow(NAME, step(NAME, "Importing from " + name, reader).build());
		}
	}

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws NoSuchMethodException {
		return processorOptions.processor(getRedisOptions());
	}
}
