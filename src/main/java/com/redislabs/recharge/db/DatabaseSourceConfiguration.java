package com.redislabs.recharge.db;

import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@Data
public class DatabaseSourceConfiguration {
	private Integer fetchSize;
	private Integer maxRows;
	private Integer queryTimeout;
	private String sql;
	private Boolean useSharedExtendedConnection;
	private Boolean verifyCursorPosition;
}
