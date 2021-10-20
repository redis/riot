package com.redis.riot.redis;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.RedisItemWriterBuilder;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.DataStructureValueReader;
import com.redis.spring.batch.support.LiveRedisItemReaderBuilder;
import com.redis.spring.batch.support.ScanRedisItemReaderBuilder;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine;

@CommandLine.Command(name = "replicate-ds", description = "Replicate a source Redis DB to a target Redis DB using data structure-specific commands")
public class ReplicateDSCommand extends AbstractReplicateCommand<DataStructure<String>> {

	@Override
	protected ScanRedisItemReaderBuilder<DataStructure<String>, DataStructureValueReader<String, String>> reader(
			AbstractRedisClient client) {
		return RedisItemReader.dataStructure(client);
	}

	@Override
	protected LiveRedisItemReaderBuilder<DataStructure<String>, DataStructureValueReader<String, String>> liveReader(
			AbstractRedisClient client) {
		return RedisItemReader.dataStructure(client).live();
	}

	@Override
	protected RedisItemWriterBuilder<String, String, DataStructure<String>> writer(AbstractRedisClient client) {
		return RedisItemWriter.dataStructure(client);
	}
}
