package com.redis.riot;

import java.io.File;
import java.time.Duration;
import java.util.Optional;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import picocli.CommandLine.Option;

public class RedisOptions {

	public static final String DEFAULT_HOST = "localhost";
	public static final int DEFAULT_PORT = 6379;

	@Option(names = { "-h",
			"--hostname" }, description = "Server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
	private String host = DEFAULT_HOST;
	@Option(names = { "-p", "--port" }, description = "Server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
	private int port = DEFAULT_PORT;
	@Option(names = { "-s",
			"--socket" }, description = "Server socket (overrides hostname and port).", paramLabel = "<socket>")
	private Optional<String> socket = Optional.empty();
	@Option(names = "--user", description = "Used to send ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private Optional<String> username = Optional.empty();
	@Option(names = { "-a",
			"--pass" }, arity = "0..1", interactive = true, description = "Password to use when connecting to the server.", paramLabel = "<password>")
	private Optional<char[]> password = Optional.empty();
	@Option(names = { "-u", "--uri" }, description = "Server URI.", paramLabel = "<uri>")
	private RedisURI uri;
	@Option(names = "--timeout", description = "Redis command timeout (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private Optional<Long> timeout = Optional.empty();
	@Option(names = { "-n", "--db" }, description = "Database number (default: ${DEFAULT-VALUE}).", paramLabel = "<db>")
	private Optional<Integer> database = Optional.empty();
	@Option(names = { "-c", "--cluster" }, description = "Enable cluster mode.")
	private boolean cluster;
	@Option(names = "--tls", description = "Establish a secure TLS connection.")
	private Optional<Boolean> tls = Optional.empty();
	@Option(names = "--insecure", description = "Allow insecure TLS connection by skipping cert validation.")
	private Optional<Boolean> insecure = Optional.empty();
	@Option(names = "--ks", description = "Path to keystore.", paramLabel = "<file>")
	private Optional<File> keystore = Optional.empty();
	@Option(names = "--ks-password", arity = "0..1", interactive = true, description = "Keystore password.", paramLabel = "<pwd>")
	private Optional<String> keystorePassword = Optional.empty();
	@Option(names = "--ts", description = "Path to truststore.", paramLabel = "<file>")
	private Optional<File> truststore = Optional.empty();
	@Option(names = "--ts-password", arity = "0..1", interactive = true, description = "Truststore password.", paramLabel = "<pwd>")
	private Optional<String> truststorePassword = Optional.empty();
	@Option(names = "--cert", description = "X.509 certificate collection in PEM format.", paramLabel = "<file>")
	private Optional<File> cert = Optional.empty();
	@Option(names = "--latency", description = "Show latency metrics.")
	private boolean showMetrics;
	@Option(names = "--no-auto-reconnect", description = "Auto reconnect on connection loss. True by default.", negatable = true)
	private boolean autoReconnect = true;
	@Option(names = "--client", description = "Client name used to connect to Redis.", paramLabel = "<name>")
	private Optional<String> clientName = Optional.empty();

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

	public void setSocket(String socket) {
		this.socket = Optional.of(socket);
	}

	public void setUsername(String username) {
		this.username = Optional.of(username);
	}

	public void setPassword(char[] password) {
		this.password = Optional.of(password);
	}

	public void setUri(RedisURI uri) {
		this.uri = uri;
	}

	public void setTimeout(long timeout) {
		this.timeout = Optional.of(timeout);
	}

	public void setDatabase(int database) {
		this.database = Optional.of(database);
	}

	public void setTls(boolean tls) {
		this.tls = Optional.of(tls);
	}

	public void setInsecure(boolean insecure) {
		this.insecure = Optional.of(insecure);
	}

	public void setKeystore(File keystore) {
		this.keystore = Optional.of(keystore);
	}

	public void setKeystorePassword(String keystorePassword) {
		this.keystorePassword = Optional.of(keystorePassword);
	}

	public void setTruststore(File truststore) {
		this.truststore = Optional.of(truststore);
	}

	public void setTruststorePassword(String truststorePassword) {
		this.truststorePassword = Optional.of(truststorePassword);
	}

	public void setCert(File cert) {
		this.cert = Optional.of(cert);
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

	public void setClientName(String clientName) {
		this.clientName = Optional.of(clientName);
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public RedisURI uri() {
		RedisURI redisURI = uri == null ? RedisURI.create(host, port) : uri;
		insecure.ifPresent(b -> redisURI.setVerifyPeer(!b));
		tls.ifPresent(redisURI::setSsl);
		socket.ifPresent(redisURI::setSocket);
		username.ifPresent(redisURI::setUsername);
		password.ifPresent(redisURI::setPassword);
		database.ifPresent(redisURI::setDatabase);
		timeout.ifPresent(t -> redisURI.setTimeout(Duration.ofSeconds(t)));
		clientName.ifPresent(redisURI::setClientName);
		return redisURI;
	}

	private ClientResources clientResources() {
		DefaultClientResources.Builder builder = DefaultClientResources.builder();
		if (showMetrics) {
			builder.commandLatencyRecorder(
					CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build()));
			builder.commandLatencyPublisherOptions(
					DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(1)).build());
		}
		return builder.build();
	}

	public SslOptions sslOptions() {
		SslOptions.Builder builder = SslOptions.builder();
		if (keystore.isPresent()) {
			if (keystorePassword.isPresent()) {
				builder.keystore(keystore.get(), keystorePassword.get().toCharArray());
			} else {
				builder.keystore(keystore.get());
			}
		}
		if (truststore.isPresent()) {
			if (truststorePassword.isPresent()) {
				builder.truststore(truststore.get(), truststorePassword.get());
			} else {
				builder.truststore(truststore.get());
			}
		}
		cert.ifPresent(builder::trustManager);
		return builder.build();
	}

	public AbstractRedisClient client() {
		if (cluster) {
			RedisModulesClusterClient client = RedisModulesClusterClient.create(clientResources(), uri());
			client.setOptions(
					ClusterClientOptions.builder().autoReconnect(autoReconnect).sslOptions(sslOptions()).build());
			return client;
		}
		RedisModulesClient client = RedisModulesClient.create(clientResources(), uri());
		client.setOptions(ClientOptions.builder().autoReconnect(autoReconnect).sslOptions(sslOptions()).build());
		return client;
	}

	@Override
	public String toString() {
		return "RedisOptions [host=" + host + ", port=" + port + ", socket=" + socket + ", username=" + username
				+ ", password=" + password + ", uri=" + uri + ", timeout=" + timeout + ", database=" + database
				+ ", cluster=" + cluster + ", tls=" + tls + ", insecure=" + insecure + ", keystore=" + keystore
				+ ", keystorePassword=" + keystorePassword + ", truststore=" + truststore + ", truststorePassword="
				+ truststorePassword + ", cert=" + cert + ", showMetrics=" + showMetrics + ", autoReconnect="
				+ autoReconnect + ", clientName=" + clientName + "]";
	}

	public static StatefulRedisModulesConnection<String, String> connect(AbstractRedisClient client) {
		if (client instanceof RedisModulesClusterClient) {
			return ((RedisModulesClusterClient) client).connect();
		}
		return ((RedisModulesClient) client).connect();
	}

}
