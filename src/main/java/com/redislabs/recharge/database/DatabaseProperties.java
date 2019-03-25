package com.redislabs.recharge.database;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

@Data
@ConfigurationProperties(prefix = "db")
public class DatabaseProperties {

	private Integer fetchSize;
	private Integer maxRows;
	private Integer queryTimeout;
	private String sql;
	private boolean useSharedExtendedConnection;
	private boolean verifyCursorPosition;
}
