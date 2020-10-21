package com.redislabs.riot;

import java.io.File;
import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import picocli.CommandLine.Option;

public class RedisConnectionOptions {

	@Option(names = { "-h",
			"--hostname" }, defaultValue = "127.0.0.1", description = "Server hostname (default: ${DEFAULT-VALUE})", paramLabel = "<host>")
	private String host;
	@Option(names = { "-p",
			"--port" }, defaultValue = "6379", description = "Server port (default: ${DEFAULT-VALUE})", paramLabel = "<port>")
	private int port;
	@Option(names = { "-s",
			"--socket" }, description = "Server socket (overrides hostname and port)", paramLabel = "<socket>")
	private String socket;
	@Option(names = "--user", description = "Used to send ACL style 'AUTH username pass'. Needs password.", paramLabel = "<username>")
	private String username;
	@Option(names = { "-a",
			"--pass" }, arity = "0..1", interactive = true, description = "Password to use when connecting to the server", paramLabel = "<password>")
	private char[] password;
	@Option(names = { "-u", "--uri" }, description = "Server URI", paramLabel = "<uri>")
	private RedisURI redisURI;
	@Option(names = { "-o",
			"--timeout" }, defaultValue = "60", description = "Redis command timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long timeout;
	@Option(names = { "-n",
			"--db" }, defaultValue = "0", description = "Database number (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int database;
	@Option(names = { "-c", "--cluster" }, description = "Enable cluster mode")
	private boolean cluster;
	@Option(names = { "-t", "--tls" }, description = "Establish a secure TLS connection")
	private boolean tls;
	@Option(names = "--ks", description = "Path to keystore", paramLabel = "<file>", hidden = true)
	private File keystore;
	@Option(names = "--ks-password", arity = "0..1", interactive = true, description = "Keystore password", paramLabel = "<pwd>", hidden = true)
	private String keystorePassword;
	@Option(names = "--ts", description = "Path to truststore", paramLabel = "<file>", hidden = true)
	private File truststore;
	@Option(names = "--ts-password", arity = "0..1", interactive = true, description = "Truststore password", paramLabel = "<pwd>", hidden = true)
	private String truststorePassword;
	@Option(names = { "-l", "--latency" }, description = "Show latency metrics")
	private boolean showMetrics;
	@Option(names = { "-m",
			"--pool" }, defaultValue = "8", description = "Max pool connections (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int poolMaxTotal;
	@Option(names = "--no-auto-reconnect", description = "Disable auto-reconnect", hidden = true)
	private boolean noAutoReconnect;
	@Option(names = "--client-name", defaultValue = "RIOT", description = "Client name (default: ${DEFAULT-VALUE})", hidden = true)
	private String clientName;

	public RedisURI getRedisURI() {
		RedisURI uri = redisURI;
		if (uri == null) {
			uri = new RedisURI();
			uri.setHost(host);
			uri.setPort(port);
			uri.setSocket(socket);
			uri.setSsl(tls);
		}
		if (username != null) {
			uri.setUsername(username);
		}
		if (password != null) {
			uri.setPassword(password);
		}
		if (database != uri.getDatabase()) {
			uri.setDatabase(database);
		}
		if (timeout != uri.getTimeout().getSeconds()) {
			uri.setTimeout(Duration.ofSeconds(timeout));
		}
		uri.setClientName(clientName);
		return uri;
	}

	private ClientResources clientResources() {
		if (showMetrics) {
			DefaultClientResources.Builder builder = DefaultClientResources.builder();
			builder.commandLatencyRecorder(
					CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build()));
			builder.commandLatencyPublisherOptions(
					DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(1)).build());
			ClientResources resources = builder.build();
			resources.eventBus().get().filter(redisEvent -> redisEvent instanceof CommandLatencyEvent)
					.cast(CommandLatencyEvent.class).subscribe(e -> System.out.println(e.getLatencies()));
			return builder.build();
		}
		return null;
	}

	private ClusterClientOptions clientOptions() {
		SslOptions.Builder sslOptionsBuilder = SslOptions.builder();
		if (keystore != null) {
			if (keystorePassword == null) {
				sslOptionsBuilder.keystore(keystore);
			} else {
				sslOptionsBuilder.keystore(keystore, keystorePassword.toCharArray());
			}
		}
		if (truststore != null) {
			if (truststorePassword == null) {
				sslOptionsBuilder.truststore(truststore);
			} else {
				sslOptionsBuilder.truststore(truststore, truststorePassword);
			}
		}
		return ClusterClientOptions.builder().autoReconnect(!noAutoReconnect).sslOptions(sslOptionsBuilder.build())
				.build();
	}

	private GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig() {
		GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(poolMaxTotal);
		return poolConfig;
	}

	public <B extends RedisConnectionBuilder<String, String, B>> B configure(
			RedisConnectionBuilder<String, String, B> builder) {
		return builder.uri(getRedisURI()).cluster(cluster).clientResources(clientResources())
				.clientOptions(clientOptions()).poolConfig(poolConfig());
	}

}
