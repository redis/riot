package com.redis.riot.core;

import java.io.File;
import java.time.Duration;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.protocol.ProtocolVersion;

public class RedisOptions {

	public static final String DEFAULT_HOST = "127.0.0.1";
	public static final int DEFAULT_PORT = 6379;
	public static final SslVerifyMode DEFAULT_VERIFY_PEER = SslVerifyMode.FULL;

	private String uri;
	private String host = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	private String socket;
	private String username;
	private char[] password;
	private Duration timeout = RedisURI.DEFAULT_TIMEOUT_DURATION;
	private int database;
	private boolean tls;
	private SslVerifyMode verifyPeer = DEFAULT_VERIFY_PEER;
	private String clientName;
	private boolean cluster;
	private Duration metricsStep;
	private boolean autoReconnect = ClientOptions.DEFAULT_AUTO_RECONNECT;
	private ProtocolVersion protocolVersion;
	private File keystore;
	private char[] keystorePassword;
	private File truststore;
	private char[] truststorePassword;
	private File keyCert;
	private File key;
	private char[] keyPassword;
	private File trustedCerts;

	public String getClientName() {
		return clientName;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
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

	public Duration getTimeout() {
		return timeout;
	}

	public void setTimeout(Duration timeout) {
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

	public SslVerifyMode getVerifyPeer() {
		return verifyPeer;
	}

	public void setVerifyPeer(SslVerifyMode mode) {
		this.verifyPeer = mode;
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

	public boolean isAutoReconnect() {
		return autoReconnect;
	}

	public void setAutoReconnect(boolean autoReconnect) {
		this.autoReconnect = autoReconnect;
	}

	public ProtocolVersion getProtocolVersion() {
		return protocolVersion;
	}

	public void setProtocolVersion(ProtocolVersion protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	public boolean isCluster() {
		return cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public Duration getMetricsStep() {
		return metricsStep;
	}

	public void setMetricsStep(Duration metricsStep) {
		this.metricsStep = metricsStep;
	}

}
