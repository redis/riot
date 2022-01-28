package com.redis.riot;

import java.util.Arrays;
import java.util.Collection;

import com.redis.spring.batch.compare.KeyComparator;
import com.redis.spring.batch.compare.KeyComparator.KeyComparatorBuilder;
import com.redis.spring.batch.compare.KeyComparator.RightComparatorBuilder;
import com.redis.spring.batch.generator.Generator;
import com.redis.spring.batch.generator.Generator.ClientGeneratorBuilder;
import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;

public abstract class AbstractRiotIntegrationTests extends AbstractRiotTests {

	private final RedisContainer redis = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG)).withKeyspaceNotifications();
	private final RedisClusterContainer redisCluster = new RedisClusterContainer(
			RedisClusterContainer.DEFAULT_IMAGE_NAME.withTag(RedisClusterContainer.DEFAULT_TAG))
					.withKeyspaceNotifications();

	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(redis, redisCluster);
	}

	protected Generator.Builder generator(RedisTestContext redis, String id) throws Exception {
		return new ClientGeneratorBuilder(redis.getClient()).id(redis.getRedisURI() + "-" + id).inMemoryJobs();
	}

	protected KeyComparatorBuilder keyComparator(RedisTestContext left, RedisTestContext right) throws Exception {
		RightComparatorBuilder rightBuilder = left(left);
		KeyComparatorBuilder comparator;
		if (right.isCluster()) {
			comparator = rightBuilder.right(right.getRedisClusterClient());
		} else {
			comparator = rightBuilder.right(right.getRedisClient());
		}
		return comparator.inMemoryJobs();
	}

	private RightComparatorBuilder left(RedisTestContext redis) {
		if (redis.isCluster()) {
			return KeyComparator.left(redis.getRedisClusterClient());
		}
		return KeyComparator.left(redis.getRedisClient());
	}
}
