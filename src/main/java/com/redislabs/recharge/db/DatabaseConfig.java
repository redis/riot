package com.redislabs.recharge.db;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import com.redislabs.recharge.RechargeConfiguration;

@Configuration
public class DatabaseConfig {

	@Autowired
	private RechargeConfiguration config;

	@Bean(name = "datasource")
	@ConfigurationProperties(prefix = "datasource")
	public DataSource dataSource() {
		return DataSourceBuilder.create().build();
	}

	@Autowired
	@Qualifier("datasource")
	DataSource dataSource;

	@Bean
	@StepScope
	public JdbcCursorItemReader<Map<String, Object>> databaseReader() {
		DatabaseConfiguration db = config.getDatasource();
		JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<Map<String, Object>>();
		builder.dataSource(dataSource);
		if (db.getFetchSize() != null) {
			builder.fetchSize(db.getFetchSize());
		}
		if (db.getMaxRows() != null) {
			builder.maxRows(db.getMaxRows());
		}
		builder.name("dbreader");
		if (db.getQueryTimeout() != null) {
			builder.queryTimeout(db.getQueryTimeout());
		}
		builder.rowMapper(new ColumnMapRowMapper());
		builder.sql(db.getSql());
		if (db.getUseSharedExtendedConnection() != null) {
			builder.useSharedExtendedConnection(db.getUseSharedExtendedConnection());
		}
		if (db.getVerifyCursorPosition() != null) {
			builder.verifyCursorPosition(db.getVerifyCursorPosition());
		}
		return builder.build();
	}

}
