package com.redis.riot;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.springframework.batch.item.ItemReader;
import org.springframework.util.unit.DataSize;

import com.redis.enterprise.Database;
import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.reader.DataStructureGeneratorItemReader;
import com.redis.spring.batch.support.JobRunner;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;

public abstract class AbstractRiotIntegrationTests extends AbstractRiotTests {

	private static final int DEFAULT_BATCH_SIZE = 50;

	private final RedisContainer redisContainer = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));

	private final RedisEnterpriseContainer redisEnterpriseContainer = new RedisEnterpriseContainer(
			RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag("latest"))
			.withDatabase(Database.name("RiotTests").memory(DataSize.ofMegabytes(90)).ossCluster(true).build());

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(redisContainer, redisEnterpriseContainer);
	}

	protected void generate(RedisTestContext redis) throws Exception {
		generate(DataStructureGeneratorItemReader.builder().build(), redis);
	}

	protected void generate(ItemReader<DataStructure<String>> reader, RedisTestContext redis) throws Exception {
		generate(DEFAULT_BATCH_SIZE, reader, redis);
	}

	protected void generate(int chunkSize, ItemReader<DataStructure<String>> reader, RedisTestContext redis)
			throws Exception {
		JobRunner jobRunner = JobRunner.inMemory();
		awaitTermination(jobRunner.run(UUID.randomUUID().toString(), chunkSize, reader,
				RedisItemWriter.dataStructure(redis.getClient()).build()));
	}

}
