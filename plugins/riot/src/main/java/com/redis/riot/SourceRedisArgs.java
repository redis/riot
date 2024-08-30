package com.redis.riot;

import com.redis.riot.core.RiotUtils;
import com.redis.spring.batch.item.redis.RedisItemReader;

import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.Option;

public class SourceRedisArgs implements RedisArgs {

	@Option(names = "--source-user", description = "Source ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = "--source-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the source server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--source-timeout", description = "Source Redis command timeout in seconds (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long timeout = DEFAULT_TIMEOUT;

	@Option(names = "--source-tls", description = "Establish a secure TLS connection to source.")
	private boolean tls;

	@Option(names = "--source-insecure", description = "Allow insecure TLS connection to source by skipping cert validation.")
	private boolean insecure;

	@Option(names = "--source-cluster", description = "Enable source cluster mode.")
	private boolean cluster;

	@Option(names = "--source-resp", description = "Redis protocol version used to connect to source: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion = DEFAULT_PROTOCOL_VERSION;

	@Option(names = "--source-pool", description = "Max pool connections used for source Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolSize = RedisItemReader.DEFAULT_POOL_SIZE;

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

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	@Override
	public long getTimeout() {
		return timeout;
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
	public String toString() {
		return "SourceRedisArgs [username=" + username + ", password=" + RiotUtils.mask(password) + ", timeout="
				+ timeout + ", tls=" + tls + ", insecure=" + insecure + ", cluster=" + cluster + ", protocolVersion="
				+ protocolVersion + ", poolSize=" + poolSize + "]";
	}

}
