package com.redis.riot.db;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;

import com.redis.riot.AbstractExportCommand;
import com.redis.riot.processor.DataStructureItemProcessor;
import com.redis.spring.batch.support.job.JobFactory;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@Command(name = "export", description = "Export to a database")
public class DatabaseExportCommand extends AbstractExportCommand<Map<String, Object>> {

	@CommandLine.Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
	private String sql;
	@Mixin
	private DataSourceOptions dataSourceOptions = new DataSourceOptions();
	@Mixin
	private DatabaseExportOptions exportOptions = new DatabaseExportOptions();

	@Override
	protected Flow flow(JobFactory jobFactory) throws Exception {
		log.info("Creating data source: {}", dataSourceOptions);
		DataSource dataSource = dataSourceOptions.dataSource();
		String dbName = dataSource.getConnection().getMetaData().getDatabaseProductName();
		log.info("Creating {} database writer: {}", dbName, exportOptions);
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<>();
		builder.itemSqlParameterSourceProvider(NullableMapSqlParameterSource::new);
		builder.dataSource(dataSource);
		builder.sql(sql);
		builder.assertUpdates(exportOptions.isAssertUpdates());
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		DataStructureItemProcessor processor = DataStructureItemProcessor.builder()
				.keyRegex(exportOptions.getKeyRegex()).build();
		return flow("db-export-flow",
				step(jobFactory.step("db-export-step"), String.format("Exporting to %s", dbName), processor, writer)
						.build());
	}

}
