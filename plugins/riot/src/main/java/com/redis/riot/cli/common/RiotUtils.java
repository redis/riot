package com.redis.riot.cli.common;

import java.time.Duration;

import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisURIBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;

public interface RiotUtils {

	static RedisURI redisURI(RedisOptions redisOptions) {
		RedisURIBuilder builder = RedisURIBuilder.create();
		if (redisOptions.getUri() != null) {
			builder.uri(redisOptions.getUri().toString());
		}
		redisOptions.getHost().ifPresent(builder::host);
		if (redisOptions.getDatabase() > 0) {
			builder.database(redisOptions.getDatabase());
		}
		if (redisOptions.getPort() > 0) {
			builder.port(redisOptions.getPort());
		}
		builder.clientName(redisOptions.getClientName());
		builder.username(redisOptions.getUsername());
		builder.password(redisOptions.getPassword());
		builder.socket(redisOptions.getSocket());
		builder.ssl(redisOptions.isTls());
		builder.sslVerifyMode(redisOptions.getTlsVerifyMode());
		redisOptions.getTimeout().ifPresent(builder::timeoutInSeconds);
		return builder.build();
	}

	static AbstractRedisClient client(RedisOptions redisOptions) {
		return client(redisURI(redisOptions), redisOptions);
	}

	static AbstractRedisClient client(RedisURI redisURI, RedisOptions redisOptions) {
		ClientBuilder builder = ClientBuilder.create(redisURI);
		builder.autoReconnect(!redisOptions.isNoAutoReconnect());
		builder.cluster(redisOptions.isCluster());
		if (redisOptions.isShowMetrics()) {
			builder.commandLatencyRecorder(
					CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build()));
			builder.commandLatencyPublisherOptions(DefaultEventPublisherOptions.builder()
					.eventEmitInterval(Duration.ofSeconds(redisOptions.getMetricsStep())).build());
		}
		builder.keystore(redisOptions.getKeystore());
		builder.keystorePassword(redisOptions.getKeystorePassword());
		builder.truststore(redisOptions.getTruststore());
		builder.truststorePassword(redisOptions.getTruststorePassword());
		builder.trustManager(redisOptions.getTrustedCerts());
		builder.key(redisOptions.getKey());
		builder.keyCert(redisOptions.getKeyCert());
		builder.keyPassword(redisOptions.getKeyPassword());
		return builder.build();
	}

}
