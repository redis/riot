package com.redislabs.recharge.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.Connector;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
@Data
public class RechargeConfiguration {

	Connector connector;
	int maxThreads = 1;
	int chunkSize = 50;
	int maxItemCount = 1000;
	KeyConfiguration key = new KeyConfiguration();
	FileConfiguration file = new FileConfiguration();
	GeneratorConfiguration generator = new GeneratorConfiguration();
	List<DataType> datatypes = new ArrayList<>();
	GeoConfiguration geo = new GeoConfiguration();
	ListConfiguration list = new ListConfiguration();
	RediSearchConfiguration redisearch = new RediSearchConfiguration();
	SetConfiguration set = new SetConfiguration();
	ZSetConfiguration zset = new ZSetConfiguration();

}
