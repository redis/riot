package com.redis.riot.redis;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redis.riot.RedisOptions;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.PollableItemReader;

import picocli.CommandLine;

@CommandLine.Command(name = "replicate", description = "Replicate a source Redis database to a target Redis database using DUMP & RESTORE")
public class ReplicateKeyDumpCommand extends AbstractReplicateCommand<KeyValue<String, byte[]>> {

	@Override
	protected ItemReader<KeyValue<String, byte[]>> reader(RedisOptions redisOptions) {
		return readerOptions
				.configure(RedisItemReader.keyDump(redisOptions.client()).poolConfig(redisOptions.poolConfig()))
				.build();
	}

	@Override
	protected PollableItemReader<KeyValue<String, byte[]>> liveReader(RedisOptions redisOptions) {
		return configure(RedisItemReader.keyDump(redisOptions.client()).poolConfig(redisOptions.poolConfig()).live())
				.build();
	}

	@Override
	protected ItemWriter<KeyValue<String, byte[]>> writer(RedisOptions redisOptions) {
		return RedisItemWriter.keyDump(redisOptions.client()).poolConfig(redisOptions.poolConfig()).build();
	}

}
