package com.redis.riot;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.support.KeyComparator;
import com.redis.spring.batch.support.KeyComparator.KeyComparatorBuilder;
import com.redis.spring.batch.support.KeyComparator.RightComparatorBuilder;
import com.redis.spring.batch.support.generator.Generator;
import com.redis.spring.batch.support.generator.Generator.GeneratorBuilder;
import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.support.ConnectionPoolSupport;

@SuppressWarnings("unchecked")
@Testcontainers
public abstract class AbstractRiotIntegrationTests extends AbstractRiotTests {

	@Container
	private static final RedisContainer REDIS = new RedisContainer().withKeyspaceNotifications();
	@Container
	private static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer().withKeyspaceNotifications();
	private static JobRepository jobRepository;
	private static PlatformTransactionManager transactionManager;

	private static final Map<RedisServer, AbstractRedisClient> CLIENTS = new HashMap<>();
	private static final Map<RedisServer, GenericObjectPool<? extends StatefulConnection<String, String>>> POOLS = new HashMap<>();
	private static final Map<RedisServer, StatefulConnection<String, String>> CONNECTIONS = new HashMap<>();
	private static final Map<RedisServer, StatefulRedisPubSubConnection<String, String>> PUBSUB_CONNECTIONS = new HashMap<>();
	private static final Map<RedisServer, BaseRedisAsyncCommands<String, String>> ASYNCS = new HashMap<>();
	private static final Map<RedisServer, BaseRedisCommands<String, String>> SYNCS = new HashMap<>();

	@SuppressWarnings("deprecation")
	@BeforeAll
	public static void setup() throws Exception {
		org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean jobRepositoryFactory = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
		jobRepository = jobRepositoryFactory.getObject();
		transactionManager = jobRepositoryFactory.getTransactionManager();
		add(REDIS, REDIS_CLUSTER);
	}

	private static void add(RedisServer... containers) {
		for (RedisServer container : containers) {
			if (container instanceof RedisClusterContainer) {
				RedisModulesClusterClient client = RedisModulesClusterClient.create(container.getRedisURI());
				CLIENTS.put(container, client);
				StatefulRedisClusterConnection<String, String> connection = client.connect();
				CONNECTIONS.put(container, connection);
				SYNCS.put(container, connection.sync());
				ASYNCS.put(container, connection.async());
				PUBSUB_CONNECTIONS.put(container, client.connectPubSub());
				POOLS.put(container, ConnectionPoolSupport.createGenericObjectPool(client::connect,
						new GenericObjectPoolConfig<>()));
			} else {
				RedisModulesClient client = RedisModulesClient.create(container.getRedisURI());
				CLIENTS.put(container, client);
				StatefulRedisConnection<String, String> connection = client.connect();
				CONNECTIONS.put(container, connection);
				SYNCS.put(container, connection.sync());
				ASYNCS.put(container, connection.async());
				PUBSUB_CONNECTIONS.put(container, client.connectPubSub());
				POOLS.put(container, ConnectionPoolSupport.createGenericObjectPool(client::connect,
						new GenericObjectPoolConfig<>()));
			}
		}
	}

	@AfterEach
	public void flushall() {
		for (BaseRedisCommands<String, String> sync : SYNCS.values()) {
			((RedisServerCommands<String, String>) sync).flushall();
		}
	}

	@AfterAll
	public static void teardown() {
		CONNECTIONS.values().forEach(StatefulConnection::close);
		PUBSUB_CONNECTIONS.values().forEach(StatefulConnection::close);
		POOLS.values().forEach(GenericObjectPool::close);
		CLIENTS.values().forEach(AbstractRedisClient::shutdown);
		CLIENTS.values().forEach(c -> c.getResources().shutdown());
		SYNCS.clear();
		ASYNCS.clear();
		CONNECTIONS.clear();
		PUBSUB_CONNECTIONS.clear();
		POOLS.clear();
		CLIENTS.clear();
	}

	protected AbstractRedisClient client(RedisServer redis) {
		return CLIENTS.get(redis);
	}

	static Stream<RedisServer> containers() {
		return Stream.of(REDIS, REDIS_CLUSTER);
	}

	static Stream<RedisContainer> standaloneContainer() {
		return Stream.of(REDIS);
	}

	protected static <T> T sync(RedisServer container) {
		return (T) SYNCS.get(container);
	}

	protected static <T> T async(RedisServer container) {
		return (T) ASYNCS.get(container);
	}

	protected static <C extends StatefulConnection<String, String>> C connection(RedisServer container) {
		return (C) CONNECTIONS.get(container);
	}

	protected static <C extends StatefulConnection<String, String>> GenericObjectPool<C> pool(RedisServer container) {
		return (GenericObjectPool<C>) POOLS.get(container);
	}

	protected GeneratorBuilder dataGenerator(RedisServer redis, String id) {
		return generator(redis, redis.getRedisURI() + "-" + id);
	}

	private GeneratorBuilder generator(RedisServer redis, String id) {
		AbstractRedisClient client = CLIENTS.get(redis);
		if (client instanceof RedisClusterClient) {
			return configureGenerator(Generator.builder((RedisClusterClient) client, id));
		}
		return configureGenerator(Generator.builder((RedisClient) client, id));
	}

	private GeneratorBuilder configureGenerator(GeneratorBuilder builder) {
		return builder.jobRepository(jobRepository).transactionManager(transactionManager);
	}

	protected KeyComparatorBuilder keyComparator(AbstractRedisClient left, AbstractRedisClient right) {
		RightComparatorBuilder rightBuilder = left(left);
		KeyComparatorBuilder comparator;
		if (right instanceof RedisClusterClient) {
			comparator = rightBuilder.right((RedisClusterClient) right);
		} else {
			comparator = rightBuilder.right((RedisClient) right);
		}
		return comparator.jobRepository(jobRepository).transactionManager(transactionManager);
	}

	private RightComparatorBuilder left(AbstractRedisClient client) {
		if (client instanceof RedisClusterClient) {
			return KeyComparator.left((RedisClusterClient) client);
		}
		return KeyComparator.left((RedisClient) client);
	}
}
