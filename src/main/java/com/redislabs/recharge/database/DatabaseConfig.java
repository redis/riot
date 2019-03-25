package com.redislabs.recharge.database;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import com.zaxxer.hikari.HikariDataSource;

@Configuration
@EnableConfigurationProperties(DatabaseProperties.class)
public class DatabaseConfig {

	@Bean("dbProps")
	@ConfigurationProperties("db")
	public DataSourceProperties dataSourceProperties() {
		return new DataSourceProperties();
	}

	@Bean
	@ConfigurationProperties("db")
	@ConditionalOnProperty("db.url")
	public HikariDataSource dataSource(@Qualifier("dbProps") DataSourceProperties properties) {
		return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
	}

	@Bean
	@StepScope
	@ConditionalOnProperty("db.sql")
	public JdbcCursorItemReader<Map<String, Object>> databaseReader(DataSource dataSource, DatabaseProperties props) {
		JdbcCursorItemReaderBuilder<Map<String, Object>> builder = new JdbcCursorItemReaderBuilder<Map<String, Object>>();
		builder.dataSource(dataSource);
		if (props.getFetchSize() != null) {
			builder.fetchSize(props.getFetchSize());
		}
		if (props.getMaxRows() != null) {
			builder.maxRows(props.getMaxRows());
		}
		builder.name("database-reader");
		if (props.getQueryTimeout() != null) {
			builder.queryTimeout(props.getQueryTimeout());
		}
		builder.rowMapper(new ColumnMapRowMapper());
		builder.sql(props.getSql());
		builder.useSharedExtendedConnection(props.isUseSharedExtendedConnection());
		builder.verifyCursorPosition(props.isVerifyCursorPosition());
		return builder.build();
	}

}
