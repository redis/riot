package com.redis.riot;

import java.time.Duration;

import io.lettuce.core.protocol.ProtocolVersion;
import lombok.ToString;
import picocli.CommandLine.Option;

@ToString(exclude = "password")
public class TargetRedisArgs implements RedisClientArgs {

	@Option(names = "--target-user", description = "Target ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = "--target-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the target server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--target-timeout", description = "Target Redis command timeout in seconds (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long timeout = DEFAULT_TIMEOUT_SECONDS;

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
	public Duration getTimeout() {
		return Duration.ofSeconds(timeout);
	}

	public void setTimeout(long timeout) {
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

}
