package com.redislabs.recharge;

import com.redislabs.recharge.db.DatabaseSourceConfiguration;
import com.redislabs.recharge.file.FileSourceConfiguration;
import com.redislabs.recharge.generator.GeneratorConfiguration;
import com.redislabs.recharge.redis.RedisSourceConfiguration;

import lombok.Data;

@Data
public class SourceConfiguration {
	private int partitions = 1;
	private long sleep;
	private int sleepNanos;
	private Integer maxItemCount;
	private GeneratorConfiguration generator;
	private DatabaseSourceConfiguration db;
	private FileSourceConfiguration file;
	private RedisSourceConfiguration redis;

	public Integer getMaxItemCountPerPartition() {
		if (maxItemCount == null) {
			return null;
		}
		return maxItemCount / partitions;
	}

}
