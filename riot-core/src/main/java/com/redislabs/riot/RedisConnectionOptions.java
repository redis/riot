package com.redislabs.riot;

import java.io.File;
import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import lombok.Getter;
import picocli.CommandLine.Option;

public class RedisConnectionOptions {

	@Option(names = { "-h",
			"--hostname" }, description = "Server hostname (default: ${DEFAULT-VALUE})", paramLabel = "<host>")
	private String host = "127.0.0.1";
	@Option(names = { "-p", "--port" }, description = "Server port (default: ${DEFAULT-VALUE})", paramLabel = "<port>")
	private int port = 6379;
	@Option(names = { "-s",
			"--socket" }, description = "Server socket (overrides hostname and port)", paramLabel = "<socket>")
	private String socket;
	@Option(names = { "-a",
			"--pass" }, arity = "0..1", interactive = true, description = "Password to use when connecting to the server", paramLabel = "<password>")
	private String password;
	@Option(names = { "-u", "--uri" }, description = "Server URI", paramLabel = "<uri>")
	private RedisURI redisURI;
	@Option(names = { "-o",
			"--timeout" }, description = "Redis command timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long timeout = RedisURI.DEFAULT_TIMEOUT;
	@Option(names = { "-n", "--db" }, description = "Database number (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int database = 0;
	@Getter
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
	@Getter
	@Option(names = { "-m",
			"--pool" }, description = "Max pool connections (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int poolMaxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
	@Option(names = "--no-auto-reconnect", description = "Disable auto-reconnect", hidden = true)
	private boolean noAutoReconnect;
	@Option(names = "--client-name", description = "Client name (default: ${DEFAULT-VALUE})", hidden = true)
	private String clientName = "RIOT";

	public RedisURI getRedisURI() {
		RedisURI uri = redisURI();
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

	private RedisURI redisURI() {
		if (redisURI == null) {
			RedisURI uri = new RedisURI();
			uri.setHost(host);
			uri.setPort(port);
			uri.setSocket(socket);
			uri.setSsl(tls);
			return uri;
		}
		return redisURI;
	}

	public ClientResources getClientResources() {
		if (showMetrics) {
			DefaultClientResources.Builder clientResourcesBuilder = DefaultClientResources.builder();
			clientResourcesBuilder
					.commandLatencyCollectorOptions(DefaultCommandLatencyCollectorOptions.builder().enable().build());
			clientResourcesBuilder.commandLatencyPublisherOptions(
					DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(1)).build());
			ClientResources resources = clientResourcesBuilder.build();
			resources.eventBus().get().filter(redisEvent -> redisEvent instanceof CommandLatencyEvent)
					.cast(CommandLatencyEvent.class).subscribe(e -> System.out.println(e.getLatencies()));
			return clientResourcesBuilder.build();
		}
		return null;
	}

	public ClusterClientOptions getClientOptions() {
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

}
