package com.redislabs.recharge;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.redislabs.recharge.db.DatabaseConfiguration;
import com.redislabs.recharge.file.FileReaderConfiguration;
import com.redislabs.recharge.file.FileType;
import com.redislabs.recharge.generator.GeneratorConfiguration;
import com.redislabs.recharge.processor.ProcessorConfiguration;
import com.redislabs.recharge.redis.RedisConfiguration;

import lombok.Data;
import lombok.ToString;

@Data
@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
@ToString
public class RechargeConfiguration {

	private Integer flushall;
	private boolean meter;
	private int chunkSize = 50;
	private int partitions = 1;
	private long sleep;
	private int sleepNanos;
	private Integer maxItemCount;
	private GeneratorConfiguration generator;
	private DatabaseConfiguration datasource;
	private FileReaderConfiguration file;
	private RedisConfiguration redis;
	private ProcessorConfiguration processor;

	public Integer getMaxItemCountPerPartition() {
		if (maxItemCount == null) {
			return null;
		}
		return maxItemCount / partitions;
	}

	@SuppressWarnings("serial")
	private Map<String, FileType> fileTypes = new LinkedHashMap<String, FileType>() {
		{
			put("dat", FileType.Delimited);
			put("csv", FileType.Delimited);
			put("txt", FileType.FixedLength);
		}
	};

}
