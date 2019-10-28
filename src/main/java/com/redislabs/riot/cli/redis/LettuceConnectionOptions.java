package com.redislabs.riot.cli.redis;

import java.io.File;
import java.time.Duration;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.DefaultClientResources.Builder;
import lombok.AllArgsConstructor;
import lombok.Builder.Default;
import lombok.Data;
import lombok.NoArgsConstructor;
import picocli.CommandLine.Option;

@lombok.Builder
@AllArgsConstructor
@NoArgsConstructor
public @Data class LettuceConnectionOptions {

	@Option(names = "--ks", description = "Path to keystore", paramLabel = "<file>")
	private File keystore;
	@Option(names = "--ks-password", arity = "0..1", interactive = true, description = "Keystore password", paramLabel = "<pwd>")
	private String keystorePassword;
	@Option(names = "--ts", description = "Path to truststore", paramLabel = "<file>")
	private File truststore;
	@Option(names = "--ts-password", arity = "0..1", interactive = true, description = "Truststore password", paramLabel = "<pwd>")
	private String truststorePassword;
	@Default
	@Option(names = "--comp-threads", description = "Number of computation threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int computationThreadPoolSize = DefaultClientResources.DEFAULT_COMPUTATION_THREADS;
	@Default
	@Option(names = "--io-threads", description = "Number of threads for I/O operations (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int ioThreadPoolSize = DefaultClientResources.DEFAULT_IO_THREADS;
	@Default
	@Option(names = "--command-timeout", description = "Timeout for sync command execution (default: ${DEFAULT-VALUE})", paramLabel = "<s>")
	private long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
	@Option(names = "--metrics", description = "Show metrics")
	private boolean showMetrics;
	@Default
	@Option(names = "--publish-on-sched", description = "Enable publish on scheduler (default: ${DEFAULT-VALUE})")
	private boolean publishOnScheduler = ClientOptions.DEFAULT_PUBLISH_ON_SCHEDULER;
	@Default
	@Option(names = "--auto-reconnect", description = "Auto-reconnect (default: ${DEFAULT-VALUE})", negatable = true)
	private boolean autoReconnect = ClientOptions.DEFAULT_AUTO_RECONNECT;
	@Default
	@Option(names = "--request-queue", description = "Per-connection request queue size (default: max)", paramLabel = "<int>")
	private int requestQueueSize = ClientOptions.DEFAULT_REQUEST_QUEUE_SIZE;
	@Default
	@Option(names = "--ssl-provider", description = "SSL provider: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private SslProvider sslProvider = SslProvider.jdk;

	public long getCommandTimeout() {
		return commandTimeout;
	}

	public ClientOptions clientOptions(boolean ssl, ClientOptions.Builder builder) {
		if (ssl) {
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
		case openssl:
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

	public ClientResources clientResources() {
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
					.cast(CommandLatencyEvent.class).subscribe(e -> System.out.println(e.getLatencies()));
		}
		return resources;
	}

}
