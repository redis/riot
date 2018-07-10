package com.redislabs.recharge.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "redisearch")
@EnableAutoConfiguration
@Data
public class RediSearchConfiguration {

	String index;
	boolean cluster;
	String host;
	String password;
	Integer port;
	Integer timeout;
	Integer poolSize = 100;

}
