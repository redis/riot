package com.redislabs.riot;

import java.io.File;
import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NoArgsConstructor;
import picocli.CommandLine.Option;

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedisOptions {

	public static final String DEFAULT_HOST = "127.0.0.1";
	public static final int DEFAULT_PORT = 6379;
	public static final int DEFAULT_DATABASE = 0;
	public static final int DEFAULT_TIMEOUT = 60;
	public static final int DEFAULT_POOL_MAX_TOTAL = 8;
	public static final String DEFAULT_CLIENT_NAME = "riot";

	@Default
	@Option(names = { "-h",
			"--hostname" }, description = "Server hostname (default: ${DEFAULT-VALUE})", paramLabel = "<host>")
	private String host = DEFAULT_HOST;
	@Default
	@Option(names = { "-p", "--port" }, description = "Server port (default: ${DEFAULT-VALUE})", paramLabel = "<port>")
	private int port = DEFAULT_PORT;
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
	@Default
	@Option(names = { "-o",
			"--timeout" }, description = "Redis command timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long timeout = DEFAULT_TIMEOUT;
	@Default
	@Option(names = { "-n", "--db" }, description = "Database number (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int database = DEFAULT_DATABASE;
	@Option(names = { "-c", "--cluster" }, description = "Enable cluster mode")
	private boolean cluster;
	@Option(names = { "-t", "--tls" }, description = "Establish a secure TLS connection")
	private boolean tls;
	@Option(names = { "--no-verify-peer" }, description = "Do not verify peers when using TLS")
	private boolean noVerifyPeer;
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
	@Default
	@Option(names = { "-m",
			"--pool" }, description = "Max pool connections (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int poolMaxTotal = DEFAULT_POOL_MAX_TOTAL;
	@Option(names = "--no-auto-reconnect", description = "Disable auto-reconnect", hidden = true)
	private boolean noAutoReconnect;
	@Default
	@Option(names = "--client-name", description = "Client name (default: ${DEFAULT-VALUE})", hidden = true)
	private String clientName = DEFAULT_CLIENT_NAME;

	public RedisURI redisURI() {
		RedisURI uri = redisURI;
		if (uri == null) {
			uri = new RedisURI();
			uri.setHost(host);
			uri.setPort(port);
			uri.setSocket(socket);
			uri.setSsl(tls);
		}
		if (noVerifyPeer) {
			uri.setVerifyPeer(false);
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
		DefaultClientResources.Builder builder = DefaultClientResources.builder();
		if (showMetrics) {
			builder.commandLatencyRecorder(
					CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build()));
			builder.commandLatencyPublisherOptions(
					DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(1)).build());
			ClientResources resources = builder.build();
			resources.eventBus().get().filter(redisEvent -> redisEvent instanceof CommandLatencyEvent)
					.cast(CommandLatencyEvent.class).subscribe(e -> System.out.println(e.getLatencies()));
		}
		return builder.build();
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

	public AbstractRedisClient client() {
		if (cluster) {
			RedisClusterClient client = RedisClusterClient.create(clientResources(), redisURI());
			client.setOptions(clientOptions());
			return client;
		}
		RedisClient client = RedisClient.create(clientResources(), redisURI());
		client.setOptions(clientOptions());
		return client;
	}

	public GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig() {
		GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(poolMaxTotal);
		return poolConfig;
	}

}
