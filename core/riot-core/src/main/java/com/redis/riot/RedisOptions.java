package com.redis.riot;

import java.io.File;
import java.time.Duration;
import java.util.Optional;

import com.redis.lettucemod.util.RedisClientOptions;
import com.redis.lettucemod.util.RedisClientOptions.Builder;
import com.redis.spring.batch.common.RedisConnectionPoolOptions;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import picocli.CommandLine.Option;

public class RedisOptions {

	public enum ReadFrom {

		MASTER, MASTER_PREFERRED, UPSTREAM, UPSTREAM_PREFERRED, REPLICA_PREFERRED, REPLICA, LOWEST_LATENCY, ANY,
		ANY_REPLICA
	}

	public static final Duration DEFAULT_METRICS_STEP = Duration.ofSeconds(5);

	@Option(names = { "-h",
			"--hostname" }, description = "Server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
	private String host = RedisClientOptions.DEFAULT_HOST;

	@Option(names = { "-p", "--port" }, description = "Server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
	private int port = RedisClientOptions.DEFAULT_PORT;

	@Option(names = { "-s",
			"--socket" }, description = "Server socket (overrides hostname and port).", paramLabel = "<socket>")
	private Optional<String> socket = Optional.empty();

	@Option(names = "--user", description = "Used to send ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = { "-a",
			"--pass" }, arity = "0..1", interactive = true, description = "Password to use when connecting to the server.", paramLabel = "<password>")
	private char[] password;

	@Option(names = { "-u", "--uri" }, description = "Server URI.", paramLabel = "<uri>")
	private RedisURI uri;

	@Option(names = "--timeout", description = "Redis command timeout.", paramLabel = "<sec>")
	private Optional<Long> timeout = Optional.empty();

	@Option(names = { "-n", "--db" }, description = "Database number.", paramLabel = "<db>")
	private int database;

	@Option(names = { "-c", "--cluster" }, description = "Enable cluster mode.")
	private boolean cluster;

	@Option(names = "--tls", description = "Establish a secure TLS connection.")
	private boolean tls;

	@Option(names = "--tls-verify", description = "How to verify peers when using TLS: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private SslVerifyMode tlsVerifyMode = RedisClientOptions.DEFAULT_SSL_VERIFY_MODE;

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

	@Option(names = "--metrics", description = "Show latency metrics.")
	private boolean showMetrics;

	@Option(names = "--metrics-step", description = "Metrics publish interval (default: ${DEFAULT-VALUE}).", paramLabel = "<secs>", hidden = true)
	private long metricsStep = DEFAULT_METRICS_STEP.toSeconds();

	@Option(names = "--no-auto-reconnect", description = "Auto reconnect on connection loss (default: ${DEFAULT-VALUE}).", negatable = true)
	private boolean autoReconnect = true;

	@Option(names = "--client", description = "Client name used to connect to Redis.", paramLabel = "<name>")
	private Optional<String> clientName = Optional.empty();

	@Option(names = "--pool", description = "Max pool connections (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolMaxTotal = 8;

	@Option(names = "--read-from", description = "Which nodes to read data from: ${COMPLETION-CANDIDATES}.", paramLabel = "<name>")
	private Optional<ReadFrom> readFrom = Optional.empty();

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
		this.username = username;
	}

	public void setPassword(char[] password) {
		this.password = password;
	}

	public void setUri(RedisURI uri) {
		this.uri = uri;
	}

	public void setTimeout(long timeout) {
		this.timeout = Optional.of(timeout);
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public void setTls(boolean tls) {
		this.tls = tls;
	}

	public void setTlsVerifyMode(SslVerifyMode tlsVerifyMode) {
		this.tlsVerifyMode = tlsVerifyMode;
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

	public Optional<String> getSocket() {
		return socket;
	}

	public void setSocket(Optional<String> socket) {
		this.socket = socket;
	}

	public Optional<Long> getTimeout() {
		return timeout;
	}

	public void setTimeout(Optional<Long> timeout) {
		this.timeout = timeout;
	}

	public Optional<File> getKeystore() {
		return keystore;
	}

	public void setKeystore(Optional<File> keystore) {
		this.keystore = keystore;
	}

	public Optional<String> getKeystorePassword() {
		return keystorePassword;
	}

	public void setKeystorePassword(Optional<String> keystorePassword) {
		this.keystorePassword = keystorePassword;
	}

	public Optional<File> getTruststore() {
		return truststore;
	}

	public void setTruststore(Optional<File> truststore) {
		this.truststore = truststore;
	}

	public Optional<String> getTruststorePassword() {
		return truststorePassword;
	}

	public void setTruststorePassword(Optional<String> truststorePassword) {
		this.truststorePassword = truststorePassword;
	}

	public Optional<File> getCert() {
		return cert;
	}

	public void setCert(Optional<File> cert) {
		this.cert = cert;
	}

	public long getMetricsStep() {
		return metricsStep;
	}

	public void setMetricsStep(long metricsStep) {
		this.metricsStep = metricsStep;
	}

	public Optional<String> getClientName() {
		return clientName;
	}

	public void setClientName(Optional<String> clientName) {
		this.clientName = clientName;
	}

	public int getPoolMaxTotal() {
		return poolMaxTotal;
	}

	public void setPoolMaxTotal(int poolMaxTotal) {
		this.poolMaxTotal = poolMaxTotal;
	}

	public Optional<ReadFrom> getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(Optional<ReadFrom> readFrom) {
		this.readFrom = readFrom;
	}

	public String getUsername() {
		return username;
	}

	public char[] getPassword() {
		return password;
	}

	public RedisURI getUri() {
		return uri;
	}

	public int getDatabase() {
		return database;
	}

	public boolean isTls() {
		return tls;
	}

	public SslVerifyMode getTlsVerifyMode() {
		return tlsVerifyMode;
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

	public RedisClientOptions redisClientOptions() {
		Builder options = RedisClientOptions.builder();
		options.autoReconnect(autoReconnect);
		options.clientName(clientName);
		options.cluster(cluster);
		if (showMetrics) {
			options.commandLatencyRecorder(
					CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build()));
			options.commandLatencyPublisherOptions(
					DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(metricsStep)).build());
		}
		options.database(database);
		options.host(host);
		options.password(password);
		options.port(port);
		options.socket(socket);
		options.ssl(tls);
		options.sslVerifyMode(tlsVerifyMode);
		timeout.ifPresent(options::timeoutInSeconds);
		options.uri(uri);
		options.username(username);
		options.keystore(keystore);
		options.keystorePassword(keystorePassword);
		options.truststore(truststore);
		options.truststorePassword(truststorePassword);
		options.cert(cert);
		return options.build();
	}

	public RedisConnectionPoolOptions poolOptions() {
		return RedisConnectionPoolOptions.builder().maxTotal(poolMaxTotal)
				.readFrom(readFrom.map(ReadFrom::name).map(io.lettuce.core.ReadFrom::valueOf)).build();
	}

}
