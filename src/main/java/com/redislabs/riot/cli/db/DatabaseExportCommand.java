package com.redislabs.riot.cli.db;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import com.redislabs.riot.cli.ExportCommand;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

@Command(name = "export", description = "Export to database")
public class DatabaseExportCommand extends ExportCommand {

	@ParentCommand
	private DatabaseConnector db;
	@Parameters(arity = "1", description = "SQL statement e.g. \"INSERT INTO people (id, name) VALUES (:ssn, :name)\"", paramLabel = "<sql>")
	private String sql;

	@Override
	protected ItemWriter<Map<String, Object>> writer() {
		DataSource dataSource = db.dataSource();
		JdbcBatchItemWriterBuilder<Map<String, Object>> builder = new JdbcBatchItemWriterBuilder<Map<String, Object>>();
		builder.itemSqlParameterSourceProvider(MapSqlParameterSource::new);
		builder.dataSource(dataSource);
		builder.sql(sql);
		JdbcBatchItemWriter<Map<String, Object>> writer = builder.build();
		writer.afterPropertiesSet();
		return writer;
	}

	@Override
	protected String name() {
		return "db-export";
	}

	@Override
	protected RedisConnectionOptions redis() {
		return db.riot().redis();
	}

}
