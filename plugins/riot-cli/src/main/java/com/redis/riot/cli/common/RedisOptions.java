package com.redis.riot.cli.common;

import java.io.File;
import java.time.Duration;
import java.util.Optional;

import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisURIBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import picocli.CommandLine.Option;

public class RedisOptions {

	public static final Duration DEFAULT_METRICS_STEP = Duration.ofSeconds(5);
	public static final SslVerifyMode DEFAULT_SSL_VERIFY_MODE = SslVerifyMode.FULL;

	@Option(names = { "-h", "--hostname" }, description = "Server hostname.", paramLabel = "<host>")
	private Optional<String> host = Optional.empty();

	@Option(names = { "-p", "--port" }, description = "Server port.", paramLabel = "<port>")
	private int port;

	@Option(names = { "-s",
			"--socket" }, description = "Server socket (overrides hostname and port).", paramLabel = "<socket>")
	private Optional<String> socket = Optional.empty();

	@Option(names = "--user", description = "ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
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

	@Option(names = "--tls-verify", description = "TLS peer-verify mode: FULL (default), NONE, CA.", paramLabel = "<name>")
	private SslVerifyMode tlsVerifyMode = DEFAULT_SSL_VERIFY_MODE;

	@Option(names = "--ks", description = "Path to keystore.", paramLabel = "<file>", hidden = true)
	private Optional<File> keystore = Optional.empty();

	@Option(names = "--ks-pwd", arity = "0..1", interactive = true, description = "Keystore password.", paramLabel = "<pwd>", hidden = true)
	private char[] keystorePassword;

	@Option(names = "--ts", description = "Path to truststore.", paramLabel = "<file>", hidden = true)
	private Optional<File> truststore = Optional.empty();

	@Option(names = "--ts-pwd", arity = "0..1", interactive = true, description = "Truststore password.", paramLabel = "<pwd>", hidden = true)
	private char[] truststorePassword;

	@Option(names = "--cert", description = "X.509 cert chain file to authenticate (PEM).", paramLabel = "<file>")
	private File keyCert;

	@Option(names = "--key", description = "PKCS#8 private key file to authenticate (PEM).", paramLabel = "<file>")
	private Optional<File> key = Optional.empty();

	@Option(names = "--key-pwd", arity = "0..1", interactive = true, description = "Private key password.", paramLabel = "<pwd>")
	private char[] keyPassword;

	@Option(names = "--cacert", description = "X.509 CA certificate file to verify with.", paramLabel = "<file>")
	private Optional<File> trustedCerts = Optional.empty();

	@Option(names = "--metrics", description = "Show latency metrics.")
	private boolean showMetrics;

	@Option(names = "--metrics-step", description = "Metrics publish interval (default: ${DEFAULT-VALUE}).", paramLabel = "<secs>", hidden = true)
	private long metricsStep = DEFAULT_METRICS_STEP.getSeconds();

	@Option(names = "--no-auto-reconnect", description = "Disable auto-reconnect on connection loss.")
	private boolean noAutoReconnect;

	@Option(names = "--client", description = "Client name used to connect to Redis.", paramLabel = "<name>")
	private Optional<String> clientName = Optional.empty();

	@Option(names = "--read-from", description = "Which nodes to read data from: ${COMPLETION-CANDIDATES}.", paramLabel = "<name>")
	private Optional<ReadFrom> readFrom = Optional.empty();

	public boolean isCluster() {
		return cluster;
	}

	public Optional<String> getHost() {
		return host;
	}

	public void setHost(String host) {
		setHost(Optional.of(host));
	}

	public void setHost(Optional<String> host) {
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

	public void setKeystorePassword(char[] keystorePassword) {
		this.keystorePassword = keystorePassword;
	}

	public void setTruststore(File truststore) {
		this.truststore = Optional.of(truststore);
	}

	public void setTruststorePassword(char[] truststorePassword) {
		this.truststorePassword = truststorePassword;
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

	public char[] getKeystorePassword() {
		return keystorePassword;
	}

	public Optional<File> getTruststore() {
		return truststore;
	}

	public void setTruststore(Optional<File> truststore) {
		this.truststore = truststore;
	}

	public char[] getTruststorePassword() {
		return truststorePassword;
	}

	public Optional<File> getTrustedCerts() {
		return trustedCerts;
	}

	public void setTrustedCerts(Optional<File> trustedCerts) {
		this.trustedCerts = trustedCerts;
	}

	public void setTrustedCerts(File certs) {
		this.trustedCerts = Optional.of(certs);
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

	public boolean isShowMetrics() {
		return showMetrics;
	}

	public void setShowMetrics(boolean showMetrics) {
		this.showMetrics = showMetrics;
	}

	public boolean isNoAutoReconnect() {
		return noAutoReconnect;
	}

	public void setNoAutoReconnect(boolean noAutoReconnect) {
		this.noAutoReconnect = noAutoReconnect;
	}

	public void setClientName(String clientName) {
		this.clientName = Optional.of(clientName);
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public RedisURI uri() {
		RedisURIBuilder builder = RedisURIBuilder.create();
		if (uri != null) {
			builder.uri(uri.toString());
		}
		host.ifPresent(builder::host);
		if (database > 0) {
			builder.database(database);
		}
		if (port > 0) {
			builder.port(port);
		}
		builder.clientName(clientName);
		builder.username(username);
		builder.password(password);
		builder.socket(socket);
		builder.ssl(tls);
		builder.sslVerifyMode(tlsVerifyMode);
		timeout.ifPresent(builder::timeoutInSeconds);
		return builder.build();
	}

	public AbstractRedisClient client() {
		ClientBuilder builder = ClientBuilder.create(uri());
		builder.autoReconnect(!noAutoReconnect);
		builder.cluster(cluster);
		if (showMetrics) {
			builder.commandLatencyRecorder(
					CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build()));
			builder.commandLatencyPublisherOptions(
					DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(metricsStep)).build());
		}

		builder.keystore(keystore);
		builder.keystorePassword(keystorePassword);
		builder.truststore(truststore);
		builder.truststorePassword(truststorePassword);
		builder.trustManager(trustedCerts);
		builder.key(key);
		builder.keyCert(keyCert);
		builder.keyPassword(keyPassword);
		return builder.build();
	}

}
