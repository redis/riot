package com.redislabs.recharge;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.file.FileReaderConfiguration;
import com.redislabs.recharge.file.FileType;
import com.redislabs.recharge.generator.GeneratorConfiguration;
import com.redislabs.recharge.processor.SpelProcessorConfiguration;
import com.redislabs.recharge.redis.RedisConfiguration;

import lombok.Data;
import lombok.ToString;

@Data
@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
@ToString
public class RechargeConfiguration {
	private boolean meter;
	private boolean flushall;
	private int flushallWait = 5;
	private int chunkSize = 50;
	private int maxItemCount = 10000;
	private int partitions = 1;
	private long sleep;
	private int sleepNanos;
	private GeneratorConfiguration generator;
	private FileReaderConfiguration file;
	private SpelProcessorConfiguration processor;
	private RedisConfiguration redis = new RedisConfiguration();

	@SuppressWarnings("serial")
	private Map<String, FileType> fileTypes = new LinkedHashMap<String, FileType>() {
		{
			put("dat", FileType.Delimited);
			put("csv", FileType.Delimited);
			put("txt", FileType.FixedLength);
		}
	};

}
