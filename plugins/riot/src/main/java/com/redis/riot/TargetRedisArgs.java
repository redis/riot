package com.redis.riot;

import java.io.File;

import com.redis.riot.core.RiotDuration;

import io.lettuce.core.protocol.ProtocolVersion;
import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class TargetRedisArgs implements RedisClientArgs {

	@Option(names = "--target-user", description = "Target ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = "--target-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the target server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--target-timeout", description = "Target Redis command timeout, e.g. 30s or 5m (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>")
	private RiotDuration timeout = DEFAULT_TIMEOUT;

	@Option(names = "--target-tls", description = "Establish a secure TLS connection to target.")
	private boolean tls;

	@Option(names = "--target-insecure", description = "Allow insecure TLS connection to target by skipping cert validation.")
	private boolean insecure;

	@Option(names = "--target-client", description = "Client name used to connect to target Redis.", paramLabel = "<name>")
	private String clientName;

	@Option(names = "--target-cluster", description = "Enable target cluster mode.")
	private boolean cluster;

	@Option(names = "--target-resp", description = "Redis protocol version used to connect to target: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion = DEFAULT_PROTOCOL_VERSION;

	@Option(names = "--target-pool", description = "Max number of target Redis connections (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolSize = DEFAULT_POOL_SIZE;

	@Option(names = "--target-read-from", description = "Which target Redis cluster nodes to read from: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<n>")
	private ReadFrom readFrom = DEFAULT_READ_FROM;

	@Option(names = "--target-keystore", description = "Path to keystore.", paramLabel = "<file>", hidden = true)
	private File keystore;

	@Option(names = "--target-keystore-pass", arity = "0..1", interactive = true, description = "Keystore password.", paramLabel = "<password>", hidden = true)
	private char[] keystorePassword;

	@Option(names = "--target-trust", description = "Path to truststore.", paramLabel = "<file>", hidden = true)
	private File truststore;

	@Option(names = "--target-trust-pass", arity = "0..1", interactive = true, description = "Truststore password.", paramLabel = "<password>", hidden = true)
	private char[] truststorePassword;

	@Option(names = "--target-cert", description = "Client certificate to authenticate with (X.509 PEM).", paramLabel = "<file>")
	private File keyCert;

	@Option(names = "--target-key", description = "Private key file to authenticate with (PKCS#8 PEM).", paramLabel = "<file>")
	private File key;

	@Option(names = "--target-key-pass", arity = "0..1", interactive = true, description = "Private key password.", paramLabel = "pw")
	private char[] keyPassword;

	@Option(names = "--target-cacert", description = "CA Certificate file to verify with (X.509).", paramLabel = "<file>")
	private File trustedCerts;

	@Override
	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	@Override
	public char[] getPassword() {
		return password;
	}

	public void setPassword(char[] password) {
		this.password = password;
	}

	@Override
	public boolean isInsecure() {
		return insecure;
	}

	public void setInsecure(boolean insecure) {
		this.insecure = insecure;
	}

	@Override
	public RiotDuration getTimeout() {
		return timeout;
	}

	public void setTimeout(RiotDuration timeout) {
		this.timeout = timeout;
	}

	@Override
	public boolean isTls() {
		return tls;
	}

	public void setTls(boolean tls) {
		this.tls = tls;
	}

	@Override
	public String getClientName() {
		return clientName;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
	}

	@Override
	public boolean isCluster() {
		return cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	@Override
	public ProtocolVersion getProtocolVersion() {
		return protocolVersion;
	}

	public void setProtocolVersion(ProtocolVersion protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	@Override
	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	@Override
	public ReadFrom getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	@Override
	public File getKeystore() {
		return keystore;
	}

	public void setKeystore(File keystore) {
		this.keystore = keystore;
	}

	@Override
	public char[] getKeystorePassword() {
		return keystorePassword;
	}

	public void setKeystorePassword(char[] keystorePassword) {
		this.keystorePassword = keystorePassword;
	}

	@Override
	public File getTruststore() {
		return truststore;
	}

	public void setTruststore(File truststore) {
		this.truststore = truststore;
	}

	@Override
	public char[] getTruststorePassword() {
		return truststorePassword;
	}

	public void setTruststorePassword(char[] truststorePassword) {
		this.truststorePassword = truststorePassword;
	}

	@Override
	public File getKeyCert() {
		return keyCert;
	}

	public void setKeyCert(File keyCert) {
		this.keyCert = keyCert;
	}

	@Override
	public File getKey() {
		return key;
	}

	public void setKey(File key) {
		this.key = key;
	}

	@Override
	public char[] getKeyPassword() {
		return keyPassword;
	}

	public void setKeyPassword(char[] keyPassword) {
		this.keyPassword = keyPassword;
	}

	@Override
	public File getTrustedCerts() {
		return trustedCerts;
	}

	public void setTrustedCerts(File trustedCerts) {
		this.trustedCerts = trustedCerts;
	}

}
