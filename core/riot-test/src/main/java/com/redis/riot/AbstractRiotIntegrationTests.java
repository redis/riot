package com.redis.riot;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.springframework.batch.item.ItemReader;

import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.reader.RandomDataStructureItemReader;
import com.redis.spring.batch.support.JobRunner;
import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;

public abstract class AbstractRiotIntegrationTests extends AbstractRiotTests {

	private static final int DEFAULT_BATCH_SIZE = 50;

	private final RedisContainer redis = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG)).withKeyspaceNotifications();
	private final RedisClusterContainer redisCluster = new RedisClusterContainer(
			RedisClusterContainer.DEFAULT_IMAGE_NAME.withTag(RedisClusterContainer.DEFAULT_TAG))
			.withKeyspaceNotifications();

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(redis, redisCluster);
	}

	protected void generate(RedisTestContext redis) throws Exception {
		generate(RandomDataStructureItemReader.builder().build(), redis);
	}

	protected void generate(ItemReader<DataStructure<String>> reader, RedisTestContext redis) throws Exception {
		generate(DEFAULT_BATCH_SIZE, reader, redis);
	}

	protected void generate(int chunkSize, ItemReader<DataStructure<String>> reader, RedisTestContext redis)
			throws Exception {
		JobRunner jobRunner = JobRunner.inMemory();
		RedisItemWriter<String, String, DataStructure<String>> writer = RedisItemWriter.client(redis.getClient())
				.string().dataStructure().xaddArgs(m -> null).build();
		awaitTermination(jobRunner.run(UUID.randomUUID().toString(), chunkSize, reader, writer));
	}

}
