package com.redislabs.recharge;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.file.FileReaderConfiguration;
import com.redislabs.recharge.generator.GeneratorConfiguration;
import com.redislabs.recharge.processor.SpelProcessorConfiguration;
import com.redislabs.recharge.redis.RedisConfiguration;

import lombok.Data;

@Data
@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
public class RechargeConfiguration {
	private String name;
	private String[] fields = new String[0];
	private boolean meter;
	private boolean flushall;
	private int flushallWait = 5;
	private int chunkSize = 50;
	private int maxItemCount = 10000;
	private int partitions = 1;
	private GeneratorConfiguration generator;
	private FileReaderConfiguration file;
	private SpelProcessorConfiguration processor;
	private List<RedisConfiguration> redis = new ArrayList<>();

}
