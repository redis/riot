package com.redislabs.riot.cli.redis;

import java.io.File;
import java.time.Duration;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redislabs.lettusearch.RediSearchClient;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.DefaultClientResources.Builder;
import picocli.CommandLine.Option;

public class LettuceConnectionOptions {

	private final Logger log = LoggerFactory.getLogger(LettuceConnectionOptions.class);

	@Option(names = "--keystore", description = "Path to keystore", paramLabel = "<file>")
	private File keystore;
	@Option(names = "--keystore-password", arity = "0..1", interactive = true, description = "Keystore password", paramLabel = "<pwd>")
	private String keystorePassword;
	@Option(names = "--truststore", description = "Path to truststore", paramLabel = "<file>")
	private File truststore;
	@Option(names = "--truststore-password", arity = "0..1", interactive = true, description = "Truststore password", paramLabel = "<pwd>")
	private String truststorePassword;
	@Option(names = "--computation-thread-pool-size", description = "Number of threads for computation operations (default value is the number of CPUs=${DEFAULT-VALUE})")
	private int computationThreadPoolSize = DefaultClientResources.DEFAULT_COMPUTATION_THREADS;
	@Option(names = "--io-thread-pool-size", description = "Number of threads for I/O operations (default value is the number of CPUs=${DEFAULT-VALUE})")
	private int ioThreadPoolSize = DefaultClientResources.DEFAULT_IO_THREADS;
	@Option(names = "--command-timeout", description = "Lettuce command timeout for synchronous command execution (default: ${DEFAULT-VALUE})", paramLabel = "<seconds>")
	private long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
	@Option(names = "--metrics", description = "Show Lettuce metrics")
	private boolean showMetrics;
	@Option(names = "--publish-on-scheduler", description = "Enable Lettuce publish on scheduler (default: ${DEFAULT-VALUE})", negatable = true)
	private boolean publishOnScheduler = ClientOptions.DEFAULT_PUBLISH_ON_SCHEDULER;
	@Option(names = "--auto-reconnect", description = "Enable Lettuce auto-reconnect (default: ${DEFAULT-VALUE})", negatable = true)
	private boolean autoReconnect = ClientOptions.DEFAULT_AUTO_RECONNECT;
	@Option(names = "--request-queue-size", description = "Per-connection request queue size (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int requestQueueSize = ClientOptions.DEFAULT_REQUEST_QUEUE_SIZE;
	@Option(names = "--ssl-provider", description = "SSL Provider: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private SslProvider sslProvider = SslProvider.Jdk;

	public RedisClient redisClient(RedisConnectionOptions options) {
		log.debug("Creating Lettuce client");
		RedisClient client = RedisClient.create(clientResources(), redisURI(options));
		client.setOptions(clientOptions(options));
		return client;
	}

	public RedisClusterClient redisClusterClient(RedisConnectionOptions options) {
		log.debug("Creating Lettuce cluster client");
		RedisClusterClient client = RedisClusterClient.create(clientResources(),
				options.getServers().stream().map(e -> redisURI(e, options)).collect(Collectors.toList()));
		ClusterClientOptions.Builder builder = ClusterClientOptions.builder();
		builder.maxRedirects(options.getMaxRedirects());
		client.setOptions((ClusterClientOptions) clientOptions(builder, options));
		return client;
	}

	public RediSearchClient rediSearchClient(RedisConnectionOptions options) {
		log.debug("Creating LettuSearch client");
		RediSearchClient client = RediSearchClient.create(clientResources(), redisURI(options));
		client.setOptions(clientOptions(options));
		return client;
	}

	private ClientOptions clientOptions(RedisConnectionOptions options) {
		return clientOptions(ClientOptions.builder(), options);
	}

	private ClientOptions clientOptions(ClientOptions.Builder builder, RedisConnectionOptions options) {
		if (options.isSsl()) {
			builder.sslOptions(sslOptions());
		}
		builder.publishOnScheduler(publishOnScheduler);
		builder.autoReconnect(autoReconnect);
		builder.requestQueueSize(requestQueueSize);
		return builder.build();
	}

	private SslOptions sslOptions() {
		SslOptions.Builder builder = SslOptions.builder();
		switch (sslProvider) {
		case OpenSsl:
			builder.openSslProvider();
			break;
		default:
			builder.jdkSslProvider();
			break;
		}
		if (keystore != null) {
			if (keystorePassword == null) {
				builder.keystore(keystore);
			} else {
				builder.keystore(keystore, keystorePassword.toCharArray());
			}
		}
		if (truststore != null) {
			if (truststorePassword == null) {
				builder.truststore(truststore);
			} else {
				builder.truststore(truststore, truststorePassword);
			}
		}
		return builder.build();
	}

	private ClientResources clientResources() {
		Builder builder = DefaultClientResources.builder();
		builder.computationThreadPoolSize(computationThreadPoolSize);
		builder.ioThreadPoolSize(ioThreadPoolSize);
		if (showMetrics) {
			builder.commandLatencyCollectorOptions(DefaultCommandLatencyCollectorOptions.builder().enable().build());
			builder.commandLatencyPublisherOptions(
					DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(1)).build());
		}
		ClientResources resources = builder.build();
		if (showMetrics) {
			resources.eventBus().get().filter(redisEvent -> redisEvent instanceof CommandLatencyEvent)
					.cast(CommandLatencyEvent.class).subscribe(e -> log.info(String.valueOf(e.getLatencies())));
		}
		return resources;
	}

	private RedisURI redisURI(RedisConnectionOptions options) {
		if (options.getSentinelMaster() == null) {
			return redisURI(options.getServers().get(0), options);
		}
		RedisURI.Builder builder = RedisURI.Builder.sentinel(options.getServers().get(0).getHost(),
				options.getServers().get(0).getPort(), options.getSentinelMaster());
		options.getServers().forEach(e -> builder.withSentinel(e.getHost(), e.getPort()));
		return redisURI(builder, options);
	}

	private RedisURI redisURI(RedisEndpoint endpoint, RedisConnectionOptions options) {
		RedisURI.Builder builder = RedisURI.Builder.redis(endpoint.getHost()).withPort(endpoint.getPort());
		return redisURI(builder, options);
	}

	private RedisURI redisURI(RedisURI.Builder builder, RedisConnectionOptions options) {
		builder.withClientName(options.getClientName()).withDatabase(options.getDatabase())
				.withTimeout(Duration.ofSeconds(commandTimeout));
		if (options.getPassword() != null) {
			builder.withPassword(options.getPassword());
		}
		if (options.isSsl()) {
			builder.withSsl(options.isSsl());
		}
		return builder.build();
	}

}
