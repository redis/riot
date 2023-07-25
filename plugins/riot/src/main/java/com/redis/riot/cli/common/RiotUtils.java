package com.redis.riot.cli.common;

import java.time.Duration;

import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisURIBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.DefaultEventPublisherOptions.Builder;
import io.lettuce.core.event.EventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.CommandLatencyRecorder;
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

	static AbstractRedisClient client(RedisURI redisURI, RedisOptions options) {
		ClientBuilder builder = ClientBuilder.create(redisURI);
		builder.autoReconnect(!options.isNoAutoReconnect());
		builder.cluster(options.isCluster());
		builder.protocolVersion(options.getProtocolVersion());
		if (options.isShowMetrics()) {
			builder.commandLatencyRecorder(latencyRecorder());
			builder.commandLatencyPublisherOptions(latencyPublisherOptions(options));
		}
		builder.keystore(options.getKeystore());
		builder.keystorePassword(options.getKeystorePassword());
		builder.truststore(options.getTruststore());
		builder.truststorePassword(options.getTruststorePassword());
		builder.trustManager(options.getTrustedCerts());
		builder.key(options.getKey());
		builder.keyCert(options.getKeyCert());
		builder.keyPassword(options.getKeyPassword());
		return builder.build();
	}

	static CommandLatencyRecorder latencyRecorder() {
		return CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build());
	}

	static EventPublisherOptions latencyPublisherOptions(RedisOptions options) {
		Builder builder = DefaultEventPublisherOptions.builder();
		builder.eventEmitInterval(Duration.ofSeconds(options.getMetricsStep()));
		return builder.build();
	}

}
