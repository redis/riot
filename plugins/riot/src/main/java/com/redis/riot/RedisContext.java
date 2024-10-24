package com.redis.riot;

import com.redis.lettucemod.RedisModulesClientBuilder;
import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.metrics.MicrometerCommandLatencyRecorder;
import io.lettuce.core.metrics.MicrometerOptions;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.resource.ClientResources;
import io.micrometer.core.instrument.Metrics;

public class RedisContext implements AutoCloseable {

	private final RedisURI uri;
	private final AbstractRedisClient client;
	private final StatefulRedisModulesConnection<String, String> connection;

	public RedisContext(RedisURI uri, AbstractRedisClient client) {
		this.uri = uri;
		this.client = client;
		this.connection = RedisModulesUtils.connection(client);
	}

	public void configure(RedisItemReader<?, ?> reader) {
		reader.setClient(client);
		reader.setDatabase(uri.getDatabase());
	}

	public void configure(RedisItemWriter<?, ?, ?> writer) {
		writer.setClient(client);
	}

	public RedisURI getUri() {
		return uri;
	}

	public AbstractRedisClient getClient() {
		return client;
	}

	public StatefulRedisModulesConnection<String, String> getConnection() {
		return connection;
	}

	@Override
	public void close() {
		if (connection != null) {
			connection.close();
		}
		if (client != null) {
			client.shutdown();
			client.getResources().shutdown();
		}
	}

	public static RedisContext create(RedisURI uri, boolean cluster, ProtocolVersion protocolVersion, SslArgs sslArgs,
			boolean metrics) {
		RedisModulesClientBuilder clientBuilder = new RedisModulesClientBuilder();
		clientBuilder.cluster(cluster);
		ClientOptions.Builder options = cluster ? ClusterClientOptions.builder() : ClientOptions.builder();
		options.protocolVersion(protocolVersion);
		if (sslArgs != null) {
			options.sslOptions(sslArgs.sslOptions());
		}
		clientBuilder.options(options.build());
		clientBuilder.uri(uri);
		if (metrics) {
			MicrometerOptions meterOptions = MicrometerOptions.create();
			MicrometerCommandLatencyRecorder recorder = new MicrometerCommandLatencyRecorder(Metrics.globalRegistry,
					meterOptions);
			clientBuilder.resources(ClientResources.builder().commandLatencyRecorder(recorder).build());
		}
		return new RedisContext(uri, clientBuilder.build());
	}

}
