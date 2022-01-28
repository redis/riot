package com.redis.riot;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import picocli.CommandLine.Option;

public class RedisOptions {

	private static final Logger log = LoggerFactory.getLogger(RedisOptions.class);

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

	public boolean isCluster() {
		return cluster;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getSocket() {
		return socket;
	}

	public void setSocket(String socket) {
		this.socket = socket;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public char[] getPassword() {
		return password;
	}

	public void setPassword(char[] password) {
		this.password = password;
	}

	public RedisURI[] getUris() {
		return uris;
	}

	public void setUris(RedisURI[] uris) {
		this.uris = uris;
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public boolean isTls() {
		return tls;
	}

	public void setTls(boolean tls) {
		this.tls = tls;
	}

	public boolean isVerifyPeer() {
		return verifyPeer;
	}

	public void setVerifyPeer(boolean verifyPeer) {
		this.verifyPeer = verifyPeer;
	}

	public File getKeystore() {
		return keystore;
	}

	public void setKeystore(File keystore) {
		this.keystore = keystore;
	}

	public String getKeystorePassword() {
		return keystorePassword;
	}

	public void setKeystorePassword(String keystorePassword) {
		this.keystorePassword = keystorePassword;
	}

	public File getTruststore() {
		return truststore;
	}

	public void setTruststore(File truststore) {
		this.truststore = truststore;
	}

	public String getTruststorePassword() {
		return truststorePassword;
	}

	public void setTruststorePassword(String truststorePassword) {
		this.truststorePassword = truststorePassword;
	}

	public File getCert() {
		return cert;
	}

	public void setCert(File cert) {
		this.cert = cert;
	}

	public boolean isShowMetrics() {
		return showMetrics;
	}

	public void setShowMetrics(boolean showMetrics) {
		this.showMetrics = showMetrics;
	}

	public boolean isAutoReconnect() {
		return autoReconnect;
	}

	public void setAutoReconnect(boolean autoReconnect) {
		this.autoReconnect = autoReconnect;
	}

	public String getClientName() {
		return clientName;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

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
			return redisModulesClusterClient().connect();
		}
		return redisModulesClient().connect();
	}

	public AbstractRedisClient client() {
		if (cluster) {
			return redisModulesClusterClient();
		}
		return redisModulesClient();
	}

	public RedisModulesClusterClient redisModulesClusterClient() {
		if (client == null) {
			log.debug("Creating Redis cluster client: {}", this);
			RedisModulesClusterClient clusterClient = RedisModulesClusterClient.create(clientResources(), uris());
			clusterClient.setOptions(
					ClusterClientOptions.builder().autoReconnect(autoReconnect).sslOptions(sslOptions()).build());
			this.client = clusterClient;
		}
		return (RedisModulesClusterClient) client;
	}

	public RedisModulesClient redisModulesClient() {
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
