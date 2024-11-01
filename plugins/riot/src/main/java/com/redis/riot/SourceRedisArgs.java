package com.redis.riot;

import java.time.Duration;

import io.lettuce.core.protocol.ProtocolVersion;
import lombok.ToString;
import picocli.CommandLine.Option;

@ToString(exclude = "password")
public class SourceRedisArgs implements RedisClientArgs {

	@Option(names = "--source-user", description = "Source ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = "--source-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the source server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--source-timeout", description = "Source Redis command timeout in seconds (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long timeout = DEFAULT_TIMEOUT_SECONDS;

	@Option(names = "--source-tls", description = "Establish a secure TLS connection to source.")
	private boolean tls;

	@Option(names = "--source-insecure", description = "Allow insecure TLS connection to source by skipping cert validation.")
	private boolean insecure;

	@Option(names = "--source-client", description = "Client name used to connect to source Redis.", paramLabel = "<name>")
	private String clientName;

	@Option(names = "--source-cluster", description = "Enable source cluster mode.")
	private boolean cluster;

	@Option(names = "--source-resp", description = "Redis protocol version used to connect to source: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion = DEFAULT_PROTOCOL_VERSION;

	@Option(names = "--source-pool", description = "Max number of source Redis connections (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolSize = DEFAULT_POOL_SIZE;

	@Option(names = "--source-read-from", description = "Which source Redis cluster nodes to read from: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<n>")
	private ReadFrom readFrom = DEFAULT_READ_FROM;

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

	public boolean isCluster() {
		return cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

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
	public Duration getTimeout() {
		return Duration.ofSeconds(timeout);
	}

	public void setTimeout(Duration timeout) {
		this.timeout = timeout.toSeconds();
	}

	public boolean isTls() {
		return tls;
	}

	public void setTls(boolean tls) {
		this.tls = tls;
	}

	public String getClientName() {
		return clientName;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
	}

	public ReadFrom getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

}
