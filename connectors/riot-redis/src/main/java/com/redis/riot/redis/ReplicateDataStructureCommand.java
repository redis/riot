package com.redis.riot.redis;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redis.riot.RedisOptions;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.PollableItemReader;

import picocli.CommandLine;

@CommandLine.Command(name = "replicate-ds", description = "Replicate a source Redis database into a target Redis database using data structure-specific commands")
public class ReplicateDataStructureCommand extends AbstractReplicateCommand<DataStructure<String>> {

	@Override
	protected ItemReader<DataStructure<String>> reader(RedisOptions redisOptions) {
		return dataStructureReader();
	}

	@Override
	protected PollableItemReader<DataStructure<String>> liveReader(RedisOptions redisOptions) {
		return configure(RedisItemReader.dataStructure(redisOptions.client()).live()).build();
	}

	@Override
	protected ItemWriter<DataStructure<String>> writer(RedisOptions redisOptions) {
		return RedisItemWriter.dataStructure(redisOptions.client()).poolConfig(redisOptions.poolConfig()).build();
	}
}
