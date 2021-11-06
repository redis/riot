package com.redis.riot;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.util.ObjectUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Option;

@Slf4j
@Data
public class RedisOptions {

	public static final String DEFAULT_HOST = "localhost";
	public static final int DEFAULT_PORT = 6379;
	public static final int DEFAULT_DATABASE = 0;
	public static final int DEFAULT_TIMEOUT = 60;

	@Option(names = { "-h",
			"--hostname" }, description = "Server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
	private String host = DEFAULT_HOST;
	@Option(names = { "-p", "--port" }, description = "Server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
	private int port = DEFAULT_PORT;
	@Option(names = { "-s",
			"--socket" }, description = "Server socket (overrides hostname and port).", paramLabel = "<socket>")
	private String socket;
	@Option(names = "--user", description = "Used to send ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;
	@Option(names = { "-a",
			"--pass" }, arity = "0..1", interactive = true, description = "Password to use when connecting to the server.", paramLabel = "<password>")
	private char[] password;
	@Option(names = { "-u", "--uri" }, arity = "1..*", description = "Server URI.", paramLabel = "<uri>")
	private RedisURI[] uris;
	@Option(names = "--timeout", description = "Redis command timeout (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long timeout = DEFAULT_TIMEOUT;
	@Option(names = { "-n", "--db" }, description = "Database number (default: ${DEFAULT-VALUE}).", paramLabel = "<db>")
	private int database = DEFAULT_DATABASE;
	@Option(names = { "-c", "--cluster" }, description = "Enable cluster mode.")
	private boolean cluster;
	@Option(names = "--tls", description = "Establish a secure TLS connection.")
	private boolean tls;
	@Option(names = "--insecure", description = "Allow insecure TLS connection by skipping cert validation.")
	private boolean verifyPeer = true;
	@Option(names = "--ks", description = "Path to keystore.", paramLabel = "<file>")
	private File keystore;
	@Option(names = "--ks-password", arity = "0..1", interactive = true, description = "Keystore password.", paramLabel = "<pwd>")
	private String keystorePassword;
	@Option(names = "--ts", description = "Path to truststore.", paramLabel = "<file>")
	private File truststore;
	@Option(names = "--ts-password", arity = "0..1", interactive = true, description = "Truststore password.", paramLabel = "<pwd>")
	private String truststorePassword;
	@Option(names = "--cert", description = "X.509 certificate collection in PEM format.", paramLabel = "<file>")
	private File cert;
	@Option(names = "--latency", description = "Show latency metrics.")
	private boolean showMetrics;
	@Option(names = "--no-auto-reconnect", description = "Auto reconnect on connection loss. True by default.", negatable = true)
	private boolean autoReconnect = true;
	@Option(names = "--client", description = "Client name used to connect to Redis.", paramLabel = "<name>")
	private String clientName;

	private AbstractRedisClient client;

	public void shutdown() {

		if (client != null) {
			client.shutdown();
			client.getResources().shutdown();
		}
	}

	public List<RedisURI> uris() {
		List<RedisURI> redisURIs = new ArrayList<>();
		if (ObjectUtils.isEmpty(uris)) {
			RedisURI uri = RedisURI.create(host, port);
			uri.setSocket(socket);
			uri.setSsl(tls);
			redisURIs.add(uri);
		} else {
			redisURIs.addAll(Arrays.asList(uris));
		}
		for (RedisURI uri : redisURIs) {
			uri.setVerifyPeer(verifyPeer);
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
			if (clientName != null) {
				uri.setClientName(clientName);
			}
		}
		return redisURIs;
	}

	private ClientResources clientResources() {
		DefaultClientResources.Builder builder = DefaultClientResources.builder();
		if (showMetrics) {
			builder.commandLatencyRecorder(
					CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build()));
			builder.commandLatencyPublisherOptions(
					DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(1)).build());
			ClientResources resources = builder.build();
			resources.eventBus().get().filter(CommandLatencyEvent.class::isInstance).cast(CommandLatencyEvent.class)
					.subscribe(e -> log.info(e.getLatencies().toString()));
		}
		return builder.build();
	}

	private SslOptions sslOptions() {
		SslOptions.Builder builder = SslOptions.builder();
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
		if (cert != null) {
			builder.trustManager(cert);
		}
		return builder.build();
	}

	public StatefulRedisModulesConnection<String, String> connect() {
		if (cluster) {
			return clusterClient().connect();
		}
		return client().connect();
	}

	public RedisModulesClusterClient clusterClient() {
		if (client == null) {
			log.debug("Creating Redis cluster client: {}", this);
			RedisModulesClusterClient clusterClient = RedisModulesClusterClient.create(clientResources(), uris());
			clusterClient.setOptions(
					ClusterClientOptions.builder().autoReconnect(autoReconnect).sslOptions(sslOptions()).build());
			this.client = clusterClient;
		}
		return (RedisModulesClusterClient) client;
	}

	public RedisModulesClient client() {
		if (client == null) {
			log.debug("Creating Redis client: {}", this);
			RedisModulesClient redisClient = RedisModulesClient.create(clientResources(), uris().get(0));
			redisClient
					.setOptions(ClientOptions.builder().autoReconnect(autoReconnect).sslOptions(sslOptions()).build());
			this.client = redisClient;
		}
		return (RedisModulesClient) client;
	}

}
