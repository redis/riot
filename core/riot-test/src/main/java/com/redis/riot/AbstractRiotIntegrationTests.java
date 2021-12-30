package com.redis.riot;

import java.util.Arrays;
import java.util.Collection;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.spring.batch.support.KeyComparator;
import com.redis.spring.batch.support.KeyComparator.KeyComparatorBuilder;
import com.redis.spring.batch.support.KeyComparator.RightComparatorBuilder;
import com.redis.spring.batch.support.generator.Generator;
import com.redis.spring.batch.support.generator.Generator.GeneratorBuilder;
import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;

@Testcontainers
public abstract class AbstractRiotIntegrationTests extends AbstractRiotTests {

	@Container
	private static final RedisContainer REDIS = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG)).withKeyspaceNotifications();
	@Container
	private static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer(
			RedisClusterContainer.DEFAULT_IMAGE_NAME.withTag(RedisClusterContainer.DEFAULT_TAG))
					.withKeyspaceNotifications();

	@Override
	protected Collection<RedisServer> servers() {
		return Arrays.asList(REDIS, REDIS_CLUSTER);
	}

	@Override
	protected Collection<RedisServer> testServers() {
		return Arrays.asList(REDIS, REDIS_CLUSTER);
	}

	protected GeneratorBuilder dataGenerator(RedisTestContext redis, String id) throws Exception {
		return generator(redis, redis.getRedisURI() + "-" + id);
	}

	private GeneratorBuilder generator(RedisTestContext redis, String id) throws Exception {
		if (redis.isCluster()) {
			return configureGenerator(Generator.client(redis.getRedisClusterClient()).id(id));
		}
		return configureGenerator(Generator.client(redis.getRedisClient()).id(id));
	}

	private GeneratorBuilder configureGenerator(GeneratorBuilder builder) throws Exception {
		return builder.inMemoryJobs();
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
