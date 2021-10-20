package com.redis.riot.redis;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.RedisItemWriterBuilder;
import com.redis.spring.batch.support.KeyDumpValueReader;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveRedisItemReaderBuilder;
import com.redis.spring.batch.support.ScanRedisItemReaderBuilder;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine;

@CommandLine.Command(name = "replicate", description = "Replicate a source Redis DB to a target Redis DB using DUMP & RESTORE")
public class ReplicateCommand extends AbstractReplicateCommand<KeyValue<String, byte[]>> {

	@Override
	protected ScanRedisItemReaderBuilder<KeyValue<String, byte[]>, KeyDumpValueReader<String, String>> reader(
			AbstractRedisClient client) {
		return RedisItemReader.keyDump(client);
	}

	@Override
	protected LiveRedisItemReaderBuilder<KeyValue<String, byte[]>, ?> liveReader(AbstractRedisClient client) {
		return RedisItemReader.keyDump(client).live();
	}

	@Override
	protected RedisItemWriterBuilder<String, String, KeyValue<String, byte[]>> writer(AbstractRedisClient client) {
		return RedisItemWriter.keyDump(client);
	}

}
