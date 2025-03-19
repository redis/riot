package com.redis.riot;

import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.reader.KeyValueRead;

import picocli.CommandLine.Option;

public class MemoryUsageArgs {

	public static final int DEFAULT_SAMPLES = KeyValueRead.DEFAULT_MEM_USAGE_SAMPLES;

	@Option(names = "--mem-limit", description = "Max mem usage for a key to be read, for example 12KB 5MB. Use 0 for no limit but still read mem usage.", paramLabel = "<size>")
	private DataSize limit;

	@Option(names = "--mem-samples", description = "Number of memory usage samples for a key (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int samples = DEFAULT_SAMPLES;

	public DataSize getLimit() {
		return limit;
	}

	public void setLimit(DataSize limit) {
		this.limit = limit;
	}

	public int getSamples() {
		return samples;
	}

	public void setSamples(int samples) {
		this.samples = samples;
	}

	public void configure(RedisItemReader<?, ?> reader) {
		@SuppressWarnings("rawtypes")
		KeyValueRead operation = (KeyValueRead) reader.getOperation();
		operation.setMemUsageLimit(limit.toBytes());
		operation.setMemUsageSamples(samples);
	}
}