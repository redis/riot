package com.redis.riot.cli;

import java.io.File;
import java.time.Duration;

import com.redis.riot.core.RedisClientOptions;

import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.Option;

public class RedisArgs {

	@Option(names = { "-u", "--uri" }, description = "Redis server URI.", paramLabel = "<uri>")
	private String uri;

	@Option(names = { "-h",
			"--host" }, description = "Server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
	private String host = RedisClientOptions.DEFAULT_HOST;

	@Option(names = { "-p", "--port" }, description = "Server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
	private int port = RedisClientOptions.DEFAULT_PORT;

	@Option(names = { "-s",
			"--socket" }, description = "Server socket (overrides hostname and port).", paramLabel = "<socket>")
	private String socket;

	@Option(names = "--user", description = "ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = { "-a",
			"--pass" }, arity = "0..1", interactive = true, description = "Password to use when connecting to the server.", paramLabel = "<password>")
	private char[] password;

	@Option(names = "--timeout", description = "Redis command timeout in seconds.", paramLabel = "<sec>")
	private long timeout;

	@Option(names = { "-n", "--db" }, description = "Database number.", paramLabel = "<db>")
	private int database;

	@Option(names = "--client", description = "Client name used to connect to Redis.", paramLabel = "<name>")
	private String clientName;

	@Option(names = "--tls", description = "Establish a secure TLS connection.")
	private boolean tls;

	@Option(names = "--insecure", description = "Allow insecure TLS connection by skipping cert validation.")
	private boolean insecure;

	@Option(names = { "-c", "--cluster" }, description = "Enable cluster mode.")
	private boolean cluster;

	@Option(names = "--metrics-step", description = "Metrics publish interval in seconds. Use 0 to disable metrics publishing. (default: 0).", paramLabel = "<secs>", hidden = true)
	private long metricsStep;

	@Option(names = "--no-auto-reconnect", description = "Disable auto-reconnect on connection loss.")
	private boolean noAutoReconnect;

	@Option(names = "--resp", description = "Redis protocol version used to connect to Redis: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
	ProtocolVersion protocolVersion;

	@Option(names = "--ks", description = "Path to keystore.", paramLabel = "<file>", hidden = true)
	private File keystore;

	@Option(names = "--ks-pwd", arity = "0..1", interactive = true, description = "Keystore password.", paramLabel = "<pwd>", hidden = true)
	private char[] keystorePassword;

	@Option(names = "--ts", description = "Path to truststore.", paramLabel = "<file>", hidden = true)
	private File truststore;

	@Option(names = "--ts-pwd", arity = "0..1", interactive = true, description = "Truststore password.", paramLabel = "<pwd>", hidden = true)
	private char[] truststorePassword;

	@Option(names = "--cert", description = "X.509 cert chain file to authenticate (PEM).", paramLabel = "<file>")
	private File keyCert;

	@Option(names = "--key", description = "PKCS#8 private key file to authenticate (PEM).", paramLabel = "<file>")
	private File key;

	@Option(names = "--key-pwd", arity = "0..1", interactive = true, description = "Private key password.", paramLabel = "<pwd>")
	private char[] keyPassword;

	@Option(names = "--cacert", description = "X.509 CA certificate file to verify with.", paramLabel = "<file>")
	File trustedCerts;

	public RedisClientOptions redisOptions() {
		RedisClientOptions options = new RedisClientOptions();
		options.setAutoReconnect(!noAutoReconnect);
		options.setCluster(cluster);
		options.setKey(key);
		options.setKeyCert(keyCert);
		options.setKeyPassword(keyPassword);
		options.setKeystore(keystore);
		options.setKeystorePassword(keystorePassword);
		if (metricsStep > 0) {
			options.setMetricsStep(Duration.ofSeconds(metricsStep));
		}
		options.setProtocolVersion(protocolVersion);
		options.setTrustedCerts(trustedCerts);
		options.setTruststore(truststore);
		options.setTruststorePassword(truststorePassword);
		options.setClientName(clientName);
		options.setDatabase(database);
		options.setHost(host);
		options.setPassword(password);
		options.setPort(port);
		options.setSocket(socket);
		if (timeout > 0) {
			options.setTimeout(Duration.ofSeconds(timeout));
		}
		options.setTls(tls);
		options.setUri(uri);
		options.setUsername(username);
		if (insecure) {
			options.setVerifyPeer(SslVerifyMode.NONE);
		}
		return options;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
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

	public String getClientName() {
		return clientName;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
	}

	public boolean isTls() {
		return tls;
	}

	public void setTls(boolean tls) {
		this.tls = tls;
	}

	public boolean isInsecure() {
		return insecure;
	}

	public void setInsecure(boolean insecure) {
		this.insecure = insecure;
	}

	public boolean isCluster() {
		return cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public long getMetricsStep() {
		return metricsStep;
	}

	public void setMetricsStep(long metricsStep) {
		this.metricsStep = metricsStep;
	}

	public boolean isNoAutoReconnect() {
		return noAutoReconnect;
	}

	public void setNoAutoReconnect(boolean noAutoReconnect) {
		this.noAutoReconnect = noAutoReconnect;
	}

	public ProtocolVersion getProtocolVersion() {
		return protocolVersion;
	}

	public void setProtocolVersion(ProtocolVersion protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	public File getKeystore() {
		return keystore;
	}

	public void setKeystore(File keystore) {
		this.keystore = keystore;
	}

	public char[] getKeystorePassword() {
		return keystorePassword;
	}

	public void setKeystorePassword(char[] keystorePassword) {
		this.keystorePassword = keystorePassword;
	}

	public File getTruststore() {
		return truststore;
	}

	public void setTruststore(File truststore) {
		this.truststore = truststore;
	}

	public char[] getTruststorePassword() {
		return truststorePassword;
	}

	public void setTruststorePassword(char[] truststorePassword) {
		this.truststorePassword = truststorePassword;
	}

	public File getKeyCert() {
		return keyCert;
	}

	public void setKeyCert(File keyCert) {
		this.keyCert = keyCert;
	}

	public File getKey() {
		return key;
	}

	public void setKey(File key) {
		this.key = key;
	}

	public char[] getKeyPassword() {
		return keyPassword;
	}

	public void setKeyPassword(char[] keyPassword) {
		this.keyPassword = keyPassword;
	}

	public File getTrustedCerts() {
		return trustedCerts;
	}

	public void setTrustedCerts(File trustedCerts) {
		this.trustedCerts = trustedCerts;
	}

}
